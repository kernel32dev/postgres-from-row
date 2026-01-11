#![doc = include_str!("../README.md")]

mod tuples;

pub use postgres_from_row_derive::FromRow;
pub use tokio_postgres;

/// A trait that allows mapping rows from [tokio-postgres](<https://docs.rs/tokio-postgres>), to other types.
pub trait FromRow: Sized {
    /// Perform the conversion.
    ///
    /// # Panics
    ///
    /// Panics if the row does not contain the expected column names.
    fn from_row(row: impl AsRow) -> Self {
        Self::try_from_row(row.as_row()).expect("could not convert column")
    }

    /// Try's to perform the conversion.
    ///
    /// Will return an error if the row does not contain the expected column names.
    fn try_from_row(row: impl AsRow) -> Result<Self, tokio_postgres::Error>;

    /// Perform the conversion on a slice of rows.
    ///
    /// # Panics
    ///
    /// Panics if the row does not contain the expected column names.
    fn from_slice(rows: &[tokio_postgres::Row]) -> Vec<Self> {
        rows.iter().map(Self::from_row).collect()
    }

    /// Try's to perform the conversion on a slice of rows.
    ///
    /// Will return an error if the row does not contain the expected column names.
    fn try_from_slice(rows: &[tokio_postgres::Row]) -> Result<Vec<Self>, tokio_postgres::Error> {
        rows.iter().map(Self::try_from_row).collect()
    }

    /// Perform the conversion on a slice of rows.
    ///
    /// # Panics
    ///
    /// Panics if the row does not contain the expected column names.
    fn from_rows(rows: Vec<tokio_postgres::Row>) -> vec_map::VecMap<Self> {
        vec_map::VecMapEx::map(rows, Self::from_row)
    }

    /// Try's to perform the conversion on a slice of rows.
    ///
    /// Will return an error if the row does not contain the expected column names.
    fn try_from_rows(
        rows: Vec<tokio_postgres::Row>,
    ) -> Result<vec_map::VecMap<Self>, tokio_postgres::Error> {
        vec_map::VecMapEx::try_map(rows, Self::try_from_row)
    }
}

/// A helper trait to allow for apis that need a `&Row` to be able to also accept a `Row` or `&&Row`
pub trait AsRow {
    fn as_row(&self) -> &tokio_postgres::Row;
}
impl AsRow for tokio_postgres::Row {
    fn as_row(&self) -> &tokio_postgres::Row {
        self
    }
}
impl<T: AsRow> AsRow for &T {
    fn as_row(&self) -> &tokio_postgres::Row {
        (*self).as_row()
    }
}

impl<T: FromRow> FromRow for Option<T> {
    fn try_from_row(row: impl AsRow) -> Result<Self, tokio_postgres::Error> {
        match T::try_from_row(row) {
            Ok(row) => Ok(Some(row)),
            Err(error)
                if std::error::Error::source(&error).is_some_and(|x| {
                    x.downcast_ref::<tokio_postgres::types::WasNull>().is_some()
                }) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}
