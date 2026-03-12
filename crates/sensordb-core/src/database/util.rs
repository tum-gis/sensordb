use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::fmt::Write;
use std::time::Duration;

pub fn get_progress_bar(len: u64, message: &str) -> ProgressBar {
    let style = ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {human_pos}/{human_len} {msg} {percent}% ({eta})")
            .expect("should work")
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                let eta_sec = Duration::from_secs(state.eta().as_secs());
                write!(w, "{}", humantime::format_duration(eta_sec)).unwrap()
            });

    ProgressBar::new(len)
        .with_message(message.to_string())
        .with_style(style)
}
