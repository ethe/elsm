mod keys;
mod schema_model;

use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Type};

use crate::{keys::PrimaryKey, schema_model::ModelAttributes};

#[proc_macro_attribute]
pub fn elsm_schema(_args: TokenStream, input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = ast.ident.clone();

    let mut attrs = ModelAttributes {
        struct_name: struct_name.clone(),
        primary_key: None,
    };
    let mut normal_field_count = 0usize;
    let mut primary_key_definitions = None;

    let mut field_definitions: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut inner_field_definitions: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut new_args_definitions: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut new_fields_definitions: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut init_inner_builders: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut inner_from_batch_arrays: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut encode_method_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut encode_size_fields: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut decode_method_fields: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut builder_append_value: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut builder_append_null: Vec<proc_macro2::TokenStream> = Vec::new();

    if let Data::Struct(data_struct) = &ast.data {
        if let Fields::Named(fields) = &data_struct.fields {
            for field in fields.named.iter() {
                let field_name = field.ident.as_ref().unwrap();
                let mut is_string = false;
                let (field_ty, mapped_type, array_ty, builder_ty) = match &field.ty {
                    Type::Path(type_path) if type_path.path.is_ident("u8") => (
                        quote!(u8),
                        quote!(DataType::UInt8),
                        quote!(UInt8Array),
                        quote!(UInt8Builder),
                    ),
                    Type::Path(type_path) if type_path.path.is_ident("u16") => (
                        quote!(u16),
                        quote!(DataType::UInt16),
                        quote!(UInt16Array),
                        quote!(UInt16Builder),
                    ),
                    Type::Path(type_path) if type_path.path.is_ident("u32") => (
                        quote!(u32),
                        quote!(DataType::UInt32),
                        quote!(UInt32Array),
                        quote!(UInt32Builder),
                    ),
                    Type::Path(type_path) if type_path.path.is_ident("u64") => (
                        quote!(u64),
                        quote!(DataType::UInt64),
                        quote!(UInt64Array),
                        quote!(UInt64Builder),
                    ),
                    Type::Path(type_path) if type_path.path.is_ident("i8") => (
                        quote!(i8),
                        quote!(DataType::Int8),
                        quote!(Int8Array),
                        quote!(Int8Builder),
                    ),
                    Type::Path(type_path) if type_path.path.is_ident("i16") => (
                        quote!(i16),
                        quote!(DataType::Int16),
                        quote!(Int16Array),
                        quote!(Int16Builder),
                    ),
                    Type::Path(type_path) if type_path.path.is_ident("i32") => (
                        quote!(i32),
                        quote!(DataType::Int32),
                        quote!(Int32Array),
                        quote!(Int32Builder),
                    ),
                    Type::Path(type_path) if type_path.path.is_ident("i64") => (
                        quote!(i64),
                        quote!(DataType::Int64),
                        quote!(Int64Array),
                        quote!(Int64Builder),
                    ),
                    Type::Path(type_path) if type_path.path.is_ident("String") => {
                        is_string = true;
                        (
                            quote!(String),
                            quote!(DataType::Utf8),
                            quote!(StringArray),
                            quote!(StringBuilder),
                        )
                    }
                    Type::Path(type_path) if type_path.path.is_ident("bool") => (
                        quote!(bool),
                        quote!(DataType::Boolean),
                        quote!(BooleanArray),
                        quote!(BooleanBuilder),
                    ),
                    _ => unreachable!(),
                };

                field_definitions.push(quote! {
                    Field::new(stringify!(#field_name), #mapped_type, false),
                });
                new_args_definitions.push(quote! {
                    #field_name: #field_ty,
                });
                new_fields_definitions.push(quote! {
                    #field_name,
                });
                encode_method_fields.push(quote! {
                    self.inner.#field_name.encode(writer).await?;
                });
                decode_method_fields.push(quote! {
                    let #field_name = #field_ty::decode(reader).await?;
                });
                encode_size_fields.push(quote! {
                    + self.inner.#field_name.size()
                });
                match attrs.parse_field(field) {
                    Ok(false) => {
                        inner_field_definitions.push(quote! {
                            Field::new(stringify!(#field_name), #mapped_type, false),
                        });
                        init_inner_builders.push(quote! { Box::new(#builder_ty::new()), });

                        let array_name = Ident::new(
                            &format!("array_{}", normal_field_count),
                            struct_name.span(),
                        );
                        inner_from_batch_arrays.push(quote! {
                            let #array_name = struct_array
                                .column(#normal_field_count)
                                .as_any()
                                .downcast_ref::<#array_ty>()
                                .unwrap();
                            let #field_name = #array_name.value(offset).to_owned();
                        });
                        builder_append_value.push({
                            let field = if is_string {
                                quote! { &schema.inner.#field_name }
                            } else {
                                quote! { schema.inner.#field_name }
                            };

                            quote! {
                                self.inner
                                    .field_builder::<#builder_ty>(#normal_field_count)
                                    .unwrap()
                                    .append_value(#field);
                            }
                        });
                        builder_append_null.push(quote! {
                            self.inner
                                .field_builder::<#builder_ty>(#normal_field_count)
                                .unwrap()
                                .append_null();
                        });
                        normal_field_count += 1;
                    }
                    Ok(true) => {
                        primary_key_definitions = Some(PrimaryKey {
                            name: field_name.clone(),
                            schema_field_token: quote! {
                                Field::new(stringify!(#field_name), #mapped_type, false),
                            },
                            base_ty: field.ty.clone(),
                            array_ty,
                            builder_ty,
                            is_string,
                        });
                    }
                    Err(err) => return TokenStream::from(err.to_compile_error()),
                }
            }
        }
    }
    let PrimaryKey {
        name: primary_key_name,
        schema_field_token,
        base_ty,
        array_ty,
        builder_ty,
        is_string,
    } = primary_key_definitions.unwrap();

    let primary_key_append_value = if is_string {
        quote! {
            primary_key
        }
    } else {
        quote! {
            *primary_key
        }
    };

    let inner_schema_name = Ident::new(
        &format!("{}_INNER_SCHEMA", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );
    let schema_name = Ident::new(
        &format!("{}_SCHEMA", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );
    let inner_fields_name = Ident::new(
        &format!("{}_INNER_FIELDS", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    let inner_struct_name = Ident::new(&format!("{}Inner", struct_name), struct_name.span());
    let builder_name = Ident::new(&format!("{}Builder", struct_name), struct_name.span());

    let gen = quote! {
        lazy_static! {
            pub static ref #inner_schema_name: SchemaRef = {
                Arc::new(arrow::datatypes::Schema::new(vec![
                    #schema_field_token
                    Field::new("inner", DataType::Struct(#inner_fields_name.clone()), true),
                ]))
            };
            pub static ref #schema_name: SchemaRef = {
                Arc::new(arrow::datatypes::Schema::new(vec![
                    #(#field_definitions)*
                ]))
            };
            pub static ref #inner_fields_name: Fields =
                Fields::from(vec![#(#inner_field_definitions)*]);
        }

        #[derive(elsm_marco::KeyAttributes)]
        #ast

        #[derive(Debug, Eq, PartialEq)]
        pub(crate) struct #inner_struct_name {
            pub(crate) inner: Arc<#struct_name>,
        }

        impl Clone for #inner_struct_name {
            fn clone(&self) -> Self {
                Self {
                    inner: Arc::clone(&self.inner),
                }
            }
        }

        impl #inner_struct_name {
            pub fn new(#(#new_args_definitions)*) -> Self {
                Self {
                    inner: Arc::new(#struct_name { #(#new_fields_definitions)* }),
                }
            }
        }

        impl Schema for #inner_struct_name {
            type PrimaryKey = #base_ty;
            type Builder = #builder_name;
            type PrimaryKeyArray = #array_ty;

            fn arrow_schema() -> SchemaRef {
                #schema_name.clone()
            }

            fn inner_schema() -> SchemaRef {
                #inner_schema_name.clone()
            }

            fn primary_key(&self) -> Self::PrimaryKey {
                self.inner.#primary_key_name.to_owned()
            }

            fn builder() -> Self::Builder {
                #builder_name {
                    #primary_key_name: Default::default(),
                    inner: StructBuilder::new(
                        #inner_fields_name.clone(),
                        vec![#(#init_inner_builders)*],
                    ),
                }
            }

            fn from_batch(batch: &RecordBatch, offset: usize) -> (Self::PrimaryKey, Option<Self>) {
                let #primary_key_name = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<#array_ty>()
                    .unwrap()
                    .value(offset)
                    .to_owned();
                let struct_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                if struct_array.is_null(offset) {
                    return (#primary_key_name, None);
                }

                #(#inner_from_batch_arrays)*
                (
                    #primary_key_name.clone(),
                    Some(#inner_struct_name {
                        inner: Arc::new(#struct_name { #(#new_fields_definitions)* }),
                    }),
                )
            }

            fn to_primary_key_array(keys: Vec<Self::PrimaryKey>) -> Self::PrimaryKeyArray {
                #array_ty::from(keys)
            }
        }

        impl Encode for #inner_struct_name {
            type Error = io::Error;

            async fn encode<W: AsyncWrite + Unpin + Send + Sync>(
                &self,
                writer: &mut W,
            ) -> Result<(), Self::Error> {
                #(#encode_method_fields)*

                Ok(())
            }

            fn size(&self) -> usize {
                0 #(#encode_size_fields)*
            }
        }

        impl Decode for #inner_struct_name {
            type Error = io::Error;

            async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
                #(#decode_method_fields)*

                Ok(#inner_struct_name {
                    inner: Arc::new(#struct_name { #(#new_fields_definitions)* }),
                })
            }
        }

        pub(crate) struct #builder_name {
            #primary_key_name: #builder_ty,
            inner: StructBuilder,
        }

        impl Builder<#inner_struct_name> for #builder_name {
            fn add(&mut self, primary_key: &<#inner_struct_name as Schema>::PrimaryKey, schema: Option<#inner_struct_name>) {
                self.#primary_key_name.append_value(#primary_key_append_value);

                if let Some(schema) = schema {
                    #(#builder_append_value)*
                    self.inner.append(true);
                } else {
                    #(#builder_append_null)*
                    self.inner.append_null();
                }
            }

            fn finish(&mut self) -> RecordBatch {
                RecordBatch::try_new(#inner_struct_name::inner_schema(), vec![Arc::new(self.#primary_key_name.finish()), Arc::new(self.inner.finish())]).unwrap()
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(KeyAttributes, attributes(primary_key))]
pub fn key_attributes(_input: TokenStream) -> TokenStream {
    let gen = quote::quote! {};
    gen.into()
}
