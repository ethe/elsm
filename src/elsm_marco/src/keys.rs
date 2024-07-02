use proc_macro2::{Ident, TokenStream};
use syn::Type;

pub(crate) struct PrimaryKey {
    pub(crate) name: Ident,
    pub(crate) schema_field_token: TokenStream,
    pub(crate) base_ty: Type,
    pub(crate) array_ty: TokenStream,
    pub(crate) builder_ty: TokenStream,
    pub(crate) is_string: bool,
}

#[derive(Clone)]
pub(crate) struct KeyDefinition {
    pub(super) struct_name: Ident,
    pub(super) field_name: Ident,
}
