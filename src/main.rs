use std::io::Write;
use std::os::fd::AsFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use gst::MessageView;
use gstreamer as gst;
use gstreamer::prelude::{ElementExt, ElementExtManual, GstBinExtManual};
use nix::poll::{PollFd, PollFlags, PollTimeout};

fn usage() {
    eprintln!(
        "Usage: qubes-streaming [--produce|--receive]

Arguments:
    --produce       Capture the first monitor and send raw video to stdout
    --receive       Receive raw video in stdin and send to Twitch
"
    );
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        .with_line_number(true)
        .with_writer(std::io::stderr)
        .init();

    let argument = match std::env::args().nth(1) {
        Some(arg) => {
            if arg != "--produce" && arg != "--receive" {
                eprintln!("ERROR: Unknown argument '{}'\n\n", arg);
                usage();
                return Ok(());
            }

            arg
        }
        None => {
            usage();
            return Ok(());
        }
    };

    gst::init()?;

    if argument == "--produce" {
        producer()
    } else {
        receiver()
    }
}

fn producer() -> anyhow::Result<()> {
    let pipeline = gst::Pipeline::new();

    let source = gst::ElementFactory::make("ximagesrc")
        .property("use-damage", false)
        .build()?;

    let videoqueue = gst::ElementFactory::make("queue").build()?;

    let fdsink = gst::ElementFactory::make("fdsink").build()?;

    pipeline
        .add_many(&[&source, &videoqueue, &fdsink])
        .context("pipeline.add_many()")?;

    gst::Element::link_many(&[&source, &videoqueue, &fdsink]).context("pipeline.link_many()")?;

    let should_exit = Arc::new(AtomicBool::new(false));

    signal_hook::flag::register(signal_hook::consts::SIGTERM, should_exit.clone())?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, should_exit.clone())?;

    pipeline
        .set_state(gst::State::Playing)
        .context("playing pipeline")?;
    tracing::debug!("playing");

    let bus = pipeline.bus().context("gstreamer pipeline without bus")?;

    let stdin = std::io::stdin();
    let stdinfd = stdin.as_fd();
    let mut fds = [PollFd::new(stdinfd, PollFlags::POLLIN)];

    let mut received_eos = false;
    let mut already_exited = false;

    while !received_eos {
        if !already_exited && should_exit.load(Ordering::Relaxed) {
            tracing::debug!("received signal");
            // send EOS wait for the pipeline to send EOS
            pipeline.send_event(gst::event::Eos::new());
            already_exited = true;
        }

        if !already_exited && nix::poll::poll(&mut fds, PollTimeout::ZERO)? != 0 {
            tracing::info!("received quit from downstream");
            // send EOS wait for the pipeline to send EOS
            pipeline.send_event(gst::event::Eos::new());
            already_exited = true;
        }

        for msg in bus.iter_timed(gst::ClockTime::from_seconds(1)) {
            tracing::info!("looping");

            match msg.view() {
                MessageView::Eos(..) => {
                    tracing::debug!("gstreamer reach EOS");
                    received_eos = true;
                    break;
                }
                MessageView::Error(err) => {
                    pipeline.set_state(gst::State::Null)?;

                    // TODO: handle error
                    received_eos = true;

                    tracing::error!(
                        "Got error from {}: {} ({})",
                        msg.src()
                            .map(|s| String::from(s.to_string()))
                            .unwrap_or_else(|| "None".into()),
                        err.error(),
                        err.debug().unwrap_or_else(|| "".into()),
                    );
                    break;
                }
                _ => (),
            }
        }
    }

    tracing::debug!("finishing pipeline");
    pipeline.set_state(gst::State::Null)?;

    Ok(())
}

/// Capture the monitor, encode and generate fragmented MP4 media
fn receiver() -> anyhow::Result<()> {
    let framerate = 25i32;

    let pipeline = gst::Pipeline::new();

    // TODO: calculate blocksize based on format, width and height
    let videosrc = gst::ElementFactory::make("fdsrc")
        .property("fd", 0i32)
        .property("blocksize", 6220800u32)
        .build()?;

    let caps = gst::Caps::builder("video/x-raw")
        .field("width", 3840i32)
        .field("height", 1080i32)
        .field("format", "BGRx")
        .field("pixel-aspect-ratio", gst::Fraction::new(1, 1))
        .field("framerate", gst::Fraction::new(framerate, 1))
        .build();

    let stdin_videoconfig = gst::ElementFactory::make("capsfilter")
        .property("caps", &caps)
        .build()?;

    let rawvideoparse = gst::ElementFactory::make("rawvideoparse")
        .property("use-sink-caps", true)
        .build()?;

    let audiosrc = gst::ElementFactory::make("pulsesrc").build()?;
    let audioconvert = gst::ElementFactory::make("audioconvert").build()?;
    let audioresample = gst::ElementFactory::make("audioresample").build()?;
    let caps = gst::Caps::builder("audio/x-raw")
        .field("rate", 48000i32)
        .field("channels", 2i32)
        .build();

    let resampleconfig = gst::ElementFactory::make("capsfilter")
        .property("caps", &caps)
        .build()?;

    let audiocompress = gst::ElementFactory::make("fdkaacenc")
        .property("bitrate", 160000i32)
        .build()?;

    let audioqueue = gst::ElementFactory::make("queue").build()?;

    let videoconvert = gst::ElementFactory::make("videoconvert")
        .property_from_str("chroma-resampler", "lanczos")
        .property_from_str("dither", "floyd-steinberg")
        .property_from_str("method", "lanczos")
        .property("envelope", 5f64)
        .build()?;

    let videoenc = gst::ElementFactory::make("nvh264enc").build()?;

    let videomuxer = gst::ElementFactory::make("flvmux")
        .property("streamable", true)
        .build()?;
    let videoqueue = gst::ElementFactory::make("queue").build()?;

    let sink = gst::ElementFactory::make("filesink")
        .property_from_str("location", "out.flv")
        .build()?;

    pipeline
        .add_many(&[
            &videosrc,
            &stdin_videoconfig,
            &audiosrc,
            &audioconvert,
            &audioresample,
            &resampleconfig,
            &audioqueue,
            &audiocompress,
            &rawvideoparse,
            // &videoconvert,
            &videoqueue,
            &videoenc,
            &videomuxer,
            &sink,
        ])
        .context("add_many()")?;

    gst::Element::link_many(&[
        &audiosrc,
        &audioconvert,
        &audioresample,
        &resampleconfig,
        &audioqueue,
        &audiocompress,
        &videomuxer,
    ])
    .context("link_many()")?;

    gst::Element::link_many(&[
        &videosrc,
        &stdin_videoconfig,
        &rawvideoparse,
        // &videoconvert,
        &videoqueue,
        &videoenc,
        &videomuxer,
    ])
    .context("link_many()")?;

    videomuxer.link(&sink)?;

    let should_exit = Arc::new(AtomicBool::new(false));

    signal_hook::flag::register(signal_hook::consts::SIGTERM, should_exit.clone())?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, should_exit.clone())?;
    signal_hook::flag::register(signal_hook::consts::SIGUSR1, should_exit.clone())?;

    pipeline
        .set_state(gst::State::Playing)
        .context("playing pipeline")?;
    tracing::debug!("playing");

    let bus = pipeline.bus().context("gstreamer pipeline without bus")?;

    let mut received_eos = false;
    let mut already_exited = false;

    while !received_eos {
        if !already_exited && should_exit.load(Ordering::Relaxed) {
            tracing::debug!("received signal");

            // tell producer to stop
            std::io::stdout().write_all(&[0xa])?;
            std::io::stdout().flush()?;

            pipeline.send_event(gst::event::Eos::new());

            // wait for the pipeline to send EOS
            already_exited = true;
        }

        for msg in bus.iter_timed(gst::ClockTime::from_seconds(1)) {
            tracing::info!("looping");

            match msg.view() {
                MessageView::Eos(..) => {
                    tracing::debug!("gstreamer reach EOS");
                    received_eos = true;
                    break;
                }
                MessageView::Error(err) => {
                    pipeline.set_state(gst::State::Null)?;

                    // TODO: handle error
                    received_eos = true;

                    tracing::error!(
                        "Got error from {}: {} ({})",
                        msg.src()
                            .map(|s| String::from(s.to_string()))
                            .unwrap_or_else(|| "None".into()),
                        err.error(),
                        err.debug().unwrap_or_else(|| "".into()),
                    );
                    break;
                }
                _ => (),
            }
        }
    }

    tracing::debug!("finishing pipeline");
    pipeline.set_state(gst::State::Null)?;

    Ok(())
}
