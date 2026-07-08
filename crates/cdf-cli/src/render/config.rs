use std::io::IsTerminal;

const DEFAULT_WIDTH: usize = 80;
const MIN_WIDTH: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DisplayMode {
    Tty,
    Headless,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RenderEnv {
    pub no_color: bool,
    pub clicolor_force: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RenderConfig {
    display_mode: DisplayMode,
    width: usize,
    env: RenderEnv,
    no_color_flag: bool,
}

impl RenderConfig {
    pub(crate) fn detect(no_color_flag: bool) -> Self {
        let display_mode = if std::io::stdout().is_terminal() {
            DisplayMode::Tty
        } else {
            DisplayMode::Headless
        };
        let width = std::env::var("COLUMNS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_WIDTH);
        let env = RenderEnv {
            no_color: std::env::var_os("NO_COLOR").is_some(),
            clicolor_force: std::env::var("CLICOLOR_FORCE")
                .map(|value| !value.is_empty() && value != "0")
                .unwrap_or(false),
        };
        Self::new(display_mode, width, env, no_color_flag)
    }

    pub(crate) fn new(
        display_mode: DisplayMode,
        width: usize,
        env: RenderEnv,
        no_color_flag: bool,
    ) -> Self {
        Self {
            display_mode,
            width: width.max(MIN_WIDTH),
            env,
            no_color_flag,
        }
    }

    #[cfg(test)]
    pub(crate) fn headless_for_width(width: usize) -> Self {
        Self::new(
            DisplayMode::Headless,
            width,
            RenderEnv {
                no_color: false,
                clicolor_force: false,
            },
            false,
        )
    }

    pub(crate) fn display_mode(&self) -> DisplayMode {
        self.display_mode
    }

    pub(crate) fn width(&self) -> usize {
        self.width
    }

    pub(crate) fn color_enabled(&self) -> bool {
        self.display_mode == DisplayMode::Tty && !self.no_color_flag && !self.env.no_color
    }

    pub(crate) fn rich_glyphs(&self) -> bool {
        self.display_mode == DisplayMode::Tty
    }

    pub(crate) fn env(&self) -> RenderEnv {
        self.env
    }

    pub(crate) fn no_color_flag(&self) -> bool {
        self.no_color_flag
    }
}
