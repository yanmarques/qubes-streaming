use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use gst::MessageView;
use gstreamer as gst;
use gstreamer::glib::object::Cast;
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

#[derive(Debug)]
struct VideoInfo {
    width: i32,
    height: i32,
    format: String,
}

fn make_videocrop() -> anyhow::Result<gst::Element> {
    let videocrop = gst::ElementFactory::make("videocrop")
        .property("left", 2i32)
        .property("right", 1922i32)
        .property("top", 18i32)
        .property("bottom", 21i32)
        .build()?;

    Ok(videocrop)
}

fn probe_videoinfo() -> anyhow::Result<VideoInfo> {
    let pipeline = gst::Pipeline::new();

    let source = gst::ElementFactory::make("ximagesrc")
        .property("use-damage", false)
        .property("num-buffers", 1)
        .build()?;

    let videocrop = make_videocrop()?;

    let sink = gst::ElementFactory::make("appsink").build()?;

    pipeline
        .add_many(&[&source, &videocrop, &sink])
        .context("pipeline.add_many()")?;

    gst::Element::link_many(&[&source, &videocrop, &sink]).context("pipeline.link_many()")?;

    let (tx, rx) = std::sync::mpsc::sync_channel(1);

    sink.dynamic_cast::<gstreamer_app::AppSink>()
        .expect("get app sink")
        .set_callbacks(
            gstreamer_app::AppSinkCallbacks::builder()
                .new_sample(move |appsink| {
                    tracing::debug!("called from sink callback");
                    let sample = appsink.pull_sample().map_err(|_| gst::FlowError::Error)?;
                    let caps = sample.caps().ok_or_else(|| gst::FlowError::Error)?;

                    let structure = caps.structure(0).ok_or(gst::FlowError::Error)?;
                    let width = structure.get::<i32>("width").map_err(|err| {
                        tracing::error!(?err);
                        gst::FlowError::Error
                    })?;

                    let height = structure.get::<i32>("height").map_err(|err| {
                        tracing::error!(?err);
                        gst::FlowError::Error
                    })?;

                    let format = structure.get::<String>("format").map_err(|err| {
                        tracing::error!(?err);
                        gst::FlowError::Error
                    })?;

                    tx.send(VideoInfo {
                        width,
                        height,
                        format,
                    })
                    .map_err(|err| {
                        tracing::error!(?err, "failed to send video info over sync channel");
                        gst::FlowError::Error
                    })?;

                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );

    pipeline
        .set_state(gst::State::Playing)
        .context("playing pipeline")?;
    tracing::debug!("playing");

    let bus = pipeline.bus().context("gstreamer pipeline without bus")?;
    for msg in bus.iter_timed(gst::ClockTime::NONE) {
        tracing::debug!("looping");

        match msg.view() {
            MessageView::Eos(..) => {
                tracing::debug!("gstreamer reach EOS");
                break;
            }
            MessageView::Error(err) => {
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

    tracing::debug!("finishing pipeline");
    pipeline.set_state(gst::State::Null)?;

    if let Ok(video_info) = rx.try_recv() {
        return Ok(video_info);
    }

    return Err(anyhow::anyhow!("unable to find video size"));
}

/// Pack the video info into bytes and send over stdout.
/// It can be received calling `recv_stream_videoinfo` if stdout and stdin are connected
fn send_stream_videoinfo(video_info: &VideoInfo) -> anyhow::Result<()> {
    let mut dest = std::io::stdout();

    let width = video_info.width.to_be_bytes();
    let height = video_info.height.to_be_bytes();
    let format_len = video_info.format.len().to_be_bytes();
    let format = video_info.format.as_bytes();

    dest.write_all(&width)?;
    dest.write_all(&height)?;
    dest.write_all(&format_len)?;
    dest.write_all(format)?;
    dest.flush()?;

    Ok(())
}

/// Unpack the video info from stdin and rebuild the video info
fn recv_stream_videoinfo() -> anyhow::Result<VideoInfo> {
    let mut src = std::io::stdin();
    let mut buffer = [0u8; 16];
    src.read_exact(&mut buffer)?;

    let width = i32::from_be_bytes(
        buffer[0..4]
            .try_into()
            .context("parsing width from stdin")?,
    );
    let height = i32::from_be_bytes(
        buffer[4..8]
            .try_into()
            .context("parsing height from stdin")?,
    );
    let format_len = usize::from_be_bytes(
        buffer[8..]
            .try_into()
            .context("parsing format len from stdin")?,
    );

    let mut format_buf = vec![0; format_len];
    src.read_exact(&mut format_buf)?;

    let format = String::from_utf8(format_buf)?;

    Ok(VideoInfo {
        width,
        height,
        format,
    })
}

fn producer() -> anyhow::Result<()> {
    let video_info = probe_videoinfo()?;
    send_stream_videoinfo(&video_info)?;

    let pipeline = gst::Pipeline::new();

    let source = gst::ElementFactory::make("ximagesrc")
        .property("use-damage", false)
        .build()?;

    let videocrop = make_videocrop()?;

    let videoqueue = gst::ElementFactory::make("queue").build()?;

    let fdsink = gst::ElementFactory::make("fdsink").build()?;

    pipeline
        .add_many(&[&source, &videocrop, &videoqueue, &fdsink])
        .context("pipeline.add_many()")?;

    gst::Element::link_many(&[&source, &videocrop, &videoqueue, &fdsink])
        .context("pipeline.link_many()")?;

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
            tracing::debug!("looping");

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
    let video_info = recv_stream_videoinfo()?;
    tracing::info!(?video_info, "received video info");

    // let blocksize = video_info.width * video_info.height *

    let framerate = 25i32;

    let pipeline = gst::Pipeline::new();

    let videosrc = gst::ElementFactory::make("fdsrc")
        .property("fd", 0i32)
        .build()?;

    let stdin_videoconfig = gst::ElementFactory::make("capsfilter")
        .property(
            "caps",
            gst::Caps::builder("video/x-raw")
                .field("width", &video_info.width)
                .field("height", &video_info.height)
                .field("format", &video_info.format)
                .field("pixel-aspect-ratio", gst::Fraction::new(1, 1))
                .field("framerate", gst::Fraction::new(framerate, 1))
                .build(),
        )
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

    // let videoconvert = gst::ElementFactory::make("videoconvert")
    //     .property_from_str("chroma-resampler", "lanczos")
    //     .property_from_str("dither", "floyd-steinberg")
    //     .property_from_str("method", "lanczos")
    //     .property("envelope", 5f64)
    //     .build()?;

    let videoconvert = gst::ElementFactory::make("videoconvert").build()?;

    let stdin_videoconfig2 = gst::ElementFactory::make("capsfilter")
        .property(
            "caps",
            gst::Caps::builder("video/x-raw")
                .field("width", &video_info.width)
                .field("height", &video_info.height)
                .field("format", &video_info.format)
                .field("pixel-aspect-ratio", gst::Fraction::new(1, 1))
                .field("framerate", gst::Fraction::new(framerate, 1))
                .build(),
        )
        .build()?;

    let has_nvcodec = gst::ElementFactory::find("nvh264enc").is_some();

    let videoconvertconfig = gst::ElementFactory::make("capsfilter")
        .property(
            "caps",
            gst::Caps::builder("video/x-raw")
                .field("format", if has_nvcodec { "NV12" } else { "I420" })
                .build(),
        )
        .build()?;

    let videoenc = if has_nvcodec {
        gst::ElementFactory::make("nvh264enc").build()?
    } else {
        gst::ElementFactory::make("openh264enc").build()?
    };

    let videoh264parse = gst::ElementFactory::make("h264parse").build()?;

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
            &stdin_videoconfig2,
            &audiosrc,
            &audioconvert,
            &audioresample,
            &resampleconfig,
            &audioqueue,
            &audiocompress,
            &rawvideoparse,
            &videoconvertconfig,
            &videoconvert,
            &videoqueue,
            &videoenc,
            &videomuxer,
            &videoh264parse,
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
        &stdin_videoconfig2,
        &videoconvert,
        &videoconvertconfig,
        &videoqueue,
        &videoenc,
        &videoh264parse,
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
            tracing::debug!("looping");

            match msg.view() {
                MessageView::Eos(..) => {
                    tracing::debug!("gstreamer reach EOS");
                    received_eos = true;
                    break;
                }
                MessageView::Error(err) => {
                    // tell producer to stop
                    std::io::stdout().write_all(&[0xa])?;
                    std::io::stdout().flush()?;

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
