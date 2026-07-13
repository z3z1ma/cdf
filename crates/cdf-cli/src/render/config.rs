use crate::terminal::{OutputChannel, PolicyMode, TerminalEnvironment, TerminalPolicy};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DisplayMode {
    Tty,
    Headless,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RenderEnv {
    pub no_color: bool,
    pub clicolor_force: bool,
    pub unicode_supported: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RenderConfig {
    display_mode: DisplayMode,
    width: usize,
    env: RenderEnv,
    policy: TerminalPolicy,
}

impl RenderConfig {
    pub(crate) fn detect(policy: &TerminalPolicy, channel: OutputChannel) -> Self {
        Self::from_environment(*policy, channel, TerminalEnvironment::detect())
    }

    pub(crate) fn from_environment(
        policy: TerminalPolicy,
        channel: OutputChannel,
        terminal: TerminalEnvironment,
    ) -> Self {
        let display_mode = if terminal.is_terminal(channel) {
            DisplayMode::Tty
        } else {
            DisplayMode::Headless
        };
        let env = RenderEnv {
            no_color: terminal.no_color,
            clicolor_force: terminal.clicolor_force,
            unicode_supported: terminal.unicode_supported,
        };
        Self::new(display_mode, terminal.width(), env, policy)
    }

    pub(crate) fn new(
        display_mode: DisplayMode,
        width: usize,
        env: RenderEnv,
        policy: TerminalPolicy,
    ) -> Self {
        Self {
            display_mode,
            width: width.max(crate::terminal::MIN_TERMINAL_WIDTH),
            env,
            policy,
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
                unicode_supported: false,
            },
            TerminalPolicy::default(),
        )
    }

    pub(crate) fn display_mode(&self) -> DisplayMode {
        self.display_mode
    }

    pub(crate) fn width(&self) -> usize {
        self.width
    }

    pub(crate) fn color_enabled(&self) -> bool {
        self.display_mode == DisplayMode::Tty
            && self.policy.color != PolicyMode::Never
            && (!self.env.no_color || self.policy.color == PolicyMode::Always)
    }

    pub(crate) fn rich_glyphs(&self) -> bool {
        match self.policy.unicode {
            PolicyMode::Always => true,
            PolicyMode::Never => false,
            PolicyMode::Auto => self.display_mode == DisplayMode::Tty && self.env.unicode_supported,
        }
    }

    pub(crate) fn env(&self) -> RenderEnv {
        self.env
    }

    pub(crate) fn no_color_flag(&self) -> bool {
        self.policy.color == PolicyMode::Never
    }

    pub(crate) fn policy(&self) -> TerminalPolicy {
        self.policy
    }
}
