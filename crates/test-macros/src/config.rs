use syn::{
    parse::{Parse, ParseStream},
    LitStr,
};

#[derive(Debug)]
pub struct Config {
    pub pattern: String,
}

impl Parse for Config {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let pattern: LitStr = input.parse()?;
        let pattern = pattern.value();

        let config = Self { pattern };

        Ok(config)
    }
}
