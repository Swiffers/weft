//! `#[weft_db_macros::test]`: a test that needs a real Postgres.
//!
//! The test is an `async fn` taking the pool it works on:
//!
//! ```ignore
//! #[weft_db_macros::test]
//! async fn a_claimed_task_is_not_claimed_twice(pool: Pool) { ... }
//! ```
//!
//! It runs through `weft_task_store::db::testing::run`, which makes a
//! database of its own on the server `$DATABASE_URL` names, hands the
//! test a pool on it, and drops the database once the test ends, passed
//! or not. The tests are told apart from the others by where they live
//! (`.config/nextest.toml`), so only `scripts/run-db-tests.sh` runs them.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return syn::Error::new(proc_macro2::Span::call_site(), "`#[weft_db_macros::test]` takes no arguments")
            .to_compile_error()
            .into();
    }
    let test = parse_macro_input!(item as ItemFn);
    if test.sig.asyncness.is_none() {
        return syn::Error::new_spanned(test.sig.fn_token, "a database test is an `async fn` taking its pool")
            .to_compile_error()
            .into();
    }
    let attrs = &test.attrs;
    let vis = &test.vis;
    let name = &test.sig.ident;
    let inputs = &test.sig.inputs;
    let body = &test.block;
    quote! {
        #(#attrs)*
        #[::core::prelude::v1::test]
        #vis fn #name() {
            // A closure, not a named function: a name would hide the test
            // file's own item of that name inside the body.
            ::weft_task_store::db::testing::run(
                ::core::concat!(::core::module_path!(), "::", ::core::stringify!(#name)),
                |#inputs| async move #body,
            );
        }
    }
    .into()
}
