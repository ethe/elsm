use std::{io, sync::Arc};

use arrow::{
    array::{
        Array, RecordBatch, StringArray, StringBuilder, StructArray, StructBuilder, UInt64Array,
        UInt64Builder,
    },
    datatypes::{DataType, Field, Fields, SchemaRef},
};
use executor::futures::{AsyncRead, AsyncWrite};
use lazy_static::lazy_static;

use crate::{
    schema::{Builder, Schema},
    serdes::{Decode, Encode},
};

lazy_static! {
    pub static ref USER_INNER_SCHEMA: SchemaRef = {
        Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("inner", DataType::Struct(USER_INNER_FIELDS.clone()), true),
        ]))
    };
    pub static ref USER_SCHEMA: SchemaRef = {
        Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    };
    pub static ref USER_INNER_FIELDS: Fields =
        Fields::from(vec![Field::new("name", DataType::Utf8, false)]);
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct User {
    pub(crate) inner: Arc<UserInner>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct UserInner {
    pub(crate) id: u64,
    pub(crate) name: String,
}

impl Clone for User {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl User {
    pub fn new(id: u64, name: String) -> User {
        User {
            inner: Arc::new(UserInner { id, name }),
        }
    }
}

impl Schema for User {
    type PrimaryKey = u64;
    type Builder = UserBuilder;
    type PrimaryKeyArray = UInt64Array;

    fn arrow_schema() -> SchemaRef {
        USER_SCHEMA.clone()
    }

    fn inner_schema() -> SchemaRef {
        USER_INNER_SCHEMA.clone()
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.inner.id
    }

    fn builder() -> Self::Builder {
        UserBuilder {
            id: Default::default(),
            inner: StructBuilder::new(
                USER_INNER_FIELDS.clone(),
                vec![Box::new(StringBuilder::new())],
            ),
        }
    }

    fn from_batch(batch: &RecordBatch, offset: usize) -> (Self::PrimaryKey, Option<Self>) {
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(offset);
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        if name_array.is_null(offset) {
            return (id, None);
        }
        let name = name_array.value(offset);

        (
            id,
            Some(User {
                inner: Arc::new(UserInner {
                    id,
                    name: name.to_string(),
                }),
            }),
        )
    }

    fn to_primary_key_array(keys: Vec<Self::PrimaryKey>) -> Self::PrimaryKeyArray {
        UInt64Array::from(keys)
    }
}

impl Encode for User {
    type Error = io::Error;

    async fn encode<W: AsyncWrite + Unpin + Send + Sync>(
        &self,
        writer: &mut W,
    ) -> Result<(), Self::Error> {
        self.inner.id.encode(writer).await?;
        self.inner.name.encode(writer).await?;

        Ok(())
    }

    fn size(&self) -> usize {
        self.inner.id.size() + self.inner.name.size()
    }
}

impl Decode for User {
    type Error = io::Error;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let id = u64::decode(reader).await?;
        let name = String::decode(reader).await?;

        Ok(User {
            inner: Arc::new(UserInner { id, name }),
        })
    }
}

pub(crate) struct UserBuilder {
    id: UInt64Builder,
    inner: StructBuilder,
}

impl Builder<User> for UserBuilder {
    fn add(&mut self, primary_key: &<User as Schema>::PrimaryKey, schema: Option<User>) {
        self.id.append_value(*primary_key);

        if let Some(schema) = schema {
            self.inner
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(&schema.inner.name);
            self.inner.append(true);
        } else {
            self.inner
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_null();
            self.inner.append_null();
        }
    }

    fn finish(&mut self) -> RecordBatch {
        let ids = self.id.finish();
        let structs = self.inner.finish();

        RecordBatch::try_new(User::inner_schema(), vec![Arc::new(ids), Arc::new(structs)]).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use executor::ExecutorBuilder;
    use tempfile::TempDir;

    use crate::{
        oracle::LocalOracle, schema::Schema, user::User, wal::provider::in_mem::InMemProvider, Db,
        DbOption,
    };

    #[test]
    fn test_user() {
        let temp_dir = TempDir::new().unwrap();

        ExecutorBuilder::new().build().unwrap().block_on(async {
            let db = Arc::new(
                Db::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption::new(temp_dir.path().to_path_buf()),
                )
                .await
                .unwrap(),
            );
            let user_0 = User::new(0, "lizeren".to_string());
            let user_1 = User::new(1, "2333".to_string());
            let user_2 = User::new(2, "ghost".to_string());

            let mut t0 = db.new_txn();

            t0.set(user_0.primary_key(), user_0.clone());
            t0.set(user_1.primary_key(), user_1.clone());
            t0.set(user_2.primary_key(), user_2.clone());

            t0.commit().await.unwrap();

            let txn = db.new_txn();

            assert_eq!(
                txn.get(&Arc::new(user_0.primary_key()), |v| v.clone())
                    .await,
                Some(user_0)
            );
            assert_eq!(
                txn.get(&Arc::new(user_1.primary_key()), |v| v.clone())
                    .await,
                Some(user_1)
            );
            assert_eq!(
                txn.get(&Arc::new(user_2.primary_key()), |v| v.clone())
                    .await,
                Some(user_2)
            );
        });
    }
}
