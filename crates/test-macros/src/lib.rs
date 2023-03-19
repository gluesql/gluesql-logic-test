mod config;

use std::{env, path::PathBuf};

use config::Config;
use glob::glob;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{quote, ToTokens};
use relative_path::RelativePath;
use syn::{parse_macro_input, Ident, ItemFn};

#[proc_macro_attribute]
pub fn glob_tests(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as ItemFn);
    let config = parse_macro_input!(attr as Config);

    let cases = expand(&item.sig.ident, config);

    let mut output = proc_macro2::TokenStream::new();

    for case in cases {
        case.to_tokens(&mut output);
    }
    item.to_tokens(&mut output);

    output.into()
}

fn expand(callee: &Ident, attr: Config) -> Vec<ItemFn> {
    let base_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("failed to get CARGO_MANIFEST_DIR"));
    let resolved_path = RelativePath::new(&attr.pattern).to_path(&base_dir);
    let pattern = resolved_path.to_string_lossy();

    let paths = glob(&pattern).expect("invalid glob pattern");

    let mut test_fns = vec![];
    for path in paths {
        let path = path.expect("failed to get path");
        let extension = path.extension().unwrap_or_default().to_string_lossy();

        let path_str = path.to_string_lossy();
        let path_for_name = path.strip_prefix(&base_dir).expect("failed strip prefix");

        let test_name = format!(
            "{}_{}",
            callee,
            path_for_name
                .to_string_lossy()
                .strip_suffix(&format!(".{}", extension))
                .expect("failed to strip suffix extension")
                .replace(['\\', '/'], "__")
        )
        .replace("___", "__");
        let test_ident = Ident::new(&test_name, Span::call_site());

        let f = syn::parse(
            quote! {
                #[allow(non_snake_case)]
                #[test]
                fn #test_ident() {
                    let path = #test_ident;
                    #callee(::std::path::PathBuf::from(#path_str));
                }
            }
            .into(),
        )
        .expect("Failed to parse test function");

        test_fns.push(f);
    }

    if test_fns.is_empty() {
        panic!("Not found test codes")
    }

    test_fns
}
