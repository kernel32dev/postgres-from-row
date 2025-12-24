use tokio_postgres::types::FromSqlOwned;

use crate::FromRow;

impl FromRow for () {
    fn try_from_row(row: impl crate::AsRow) -> Result<Self, tokio_postgres::Error> {
        let _ = row;
        Ok(())
    }
}
macro_rules! impl_from_row_for_tuple {
    ($($T:ident),*) => {
        impl<$($T: FromSqlOwned),*> FromRow for ($($T,)*) {
            fn try_from_row(row: impl crate::AsRow) -> Result<Self, tokio_postgres::Error> {
                let row = row.as_row();
                let mut i = 0;

                #[allow(unused_assignments)]
                Ok(($(
                    row.try_get::<_, $T>({
                        let j = i;
                        i += 1;
                        j
                    })?,
                )*))
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
