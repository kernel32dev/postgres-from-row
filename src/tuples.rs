use tokio_postgres::types::{FromSqlOwned, FromSql};

use crate::FromRow;

impl FromRow for () {
    const COLUMN_COUNT: usize = 0;
    fn try_from_row_joined(_: Option<&mut Self>, _: &tokio_postgres::Row, _: usize) -> Result<Option<Self>, tokio_postgres::Error> {
        Ok(Some(()))
    }
    fn report_expected_columns() -> crate::ExpectedColumns {
        crate::ExpectedColumns::Borrowed(&[])
    }
    fn try_assert_matches(_: &[tokio_postgres::Column]) -> Result<(), ()> {
        // ignore incoming columns
        Ok(())
    }
}

macro_rules! count_ident {
    ($i:ident) => {1};
}

macro_rules! impl_from_row_for_tuple {
    ($($T:ident),*) => {
        impl<$($T: FromSqlOwned),*> FromRow for ($($T,)*) {
            const COLUMN_COUNT: usize = 0 $( + count_ident!($T))*;
            fn try_from_row_joined(_: Option<&mut Self>, row: &tokio_postgres::Row, mut i: usize) -> Result<Option<Self>, tokio_postgres::Error> {
                #[allow(unused_assignments)]
                Ok(Some(($(
                    row.try_get::<_, $T>({
                        let j = i;
                        i += 1;
                        j
                    })?,
                )*)))
            }
            fn report_expected_columns() -> crate::ExpectedColumns {
                crate::ExpectedColumns::Borrowed(const {
                    &[$(crate::ExpectedColumn::new::<$T>(None),)*]
                })
            }
            fn try_assert_matches(columns: &[tokio_postgres::Column]) -> Result<(), ()> {
                #[allow(non_snake_case)]
                let [$($T,)*] = columns else {return Err(())};
                if true $(&& <$T as FromSql>::accepts($T.type_()))* {
                    Ok(())
                } else {
                    Err(())
                }
            }
        }
    };
}

macro_rules! generate_from_row_tuples {
    () => {};
    ($head:ident $(, $tail:ident)*) => {
        impl_from_row_for_tuple!($head $(, $tail)*);
        generate_from_row_tuples!($($tail),*);
    };
}

generate_from_row_tuples!(
    T31, T30, T29, T28, T27, T26, T25, T24, T23, T22, T21, T20, T19, T18, T17, T16, T15, T14, T13,
    T12, T11, T10, T9, T8, T7, T6, T5, T4, T3, T2, T1, T0
);
