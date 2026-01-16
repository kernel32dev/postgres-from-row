#![doc = include_str!("../README.md")]

mod tuples;

pub use postgres_from_row_derive::FromRow;
pub use tokio_postgres;

pub type ExpectedColumns = std::borrow::Cow<'static, [ExpectedColumn]>;

#[derive(Debug, Clone, Copy)]
pub struct ExpectedColumn {
    column_name: Option<&'static str>,
    type_name: fn() -> &'static str,
    accepts: fn(&tokio_postgres::types::Type) -> bool,
    nullable: fn(&tokio_postgres::types::Type) -> bool,
}

impl ExpectedColumn {
    pub fn column_name(&self) -> Option<&'static str> {
        self.column_name
    }
    pub fn type_name(&self) -> &'static str {
        (self.type_name)()
    }
    pub fn accepts(&self, ty: &tokio_postgres::types::Type) -> bool {
        (self.accepts)(ty)
    }
    pub fn nullable(&self, ty: &tokio_postgres::types::Type) -> bool {
        (self.nullable)(ty)
    }
    pub fn set_nullable(&mut self) {
        self.nullable = |_| true;
    }
    pub const fn new<T: for<'a> tokio_postgres::types::FromSql<'a>>(
        column_name: Option<&'static str>,
    ) -> Self {
        Self {
            column_name,
            type_name: std::any::type_name::<T>,
            accepts: T::accepts,
            nullable: |ty| T::from_sql_null(ty).is_ok(),
        }
    }
}

pub fn report_expected_columns_mismatch(
    found_cols: &[tokio_postgres::Column],
    expected_cols: &[ExpectedColumn],
) -> String {
    use similar::{ChangeTag, TextDiff};
    use std::fmt::Write;
    let mut report = String::new();

    // TODO! update this code to correctly handle absent column names

    // 1. Prepare the sequences for diffing (just the names)
    let found_names: Vec<&str> = found_cols.iter().map(|c| c.name()).collect();
    let expected_names: Vec<&str> = expected_cols
        .iter()
        .map(|e| e.column_name().unwrap_or("-"))
        .collect();

    let diff = TextDiff::from_slices(&expected_names, &found_names);

    writeln!(report, "Column Mismatch Report:").unwrap();
    writeln!(report, "{:-<60}", "").unwrap();
    writeln!(
        report,
        "{:1} {:<20} | {:<15} | {:<15} | {}",
        "", "Column Name", "Type Match", "Nullable", "Notes"
    )
    .unwrap();
    writeln!(report, "{:-<60}", "").unwrap();

    // 2. Iterate through the diff changes
    // TextDiff tracks the indices for us so we can pull the full objects
    for change in diff.iter_all_changes() {
        let tag = change.tag();

        match tag {
            ChangeTag::Equal => {
                // Name matches! Now check if the actual type 'accepts' the expected type
                let f_idx = change.new_index().unwrap();
                let e_idx = change.old_index().unwrap();

                let f_col = &found_cols[f_idx];
                let e_col = &expected_cols[e_idx];

                let type_matches = e_col.accepts(f_col.type_());
                let is_nullable = e_col.nullable(f_col.type_());

                let status = if type_matches { "OK" } else { "MISMATCH" };

                writeln!(
                    report,
                    "  {:<20} | {:<15} | {:<15} | {}",
                    f_col.name(),
                    status,
                    if is_nullable { "Yes" } else { "No" },
                    if type_matches {
                        ""
                    } else {
                        "Type rejected the database column"
                    }
                )
                .unwrap();
            }
            ChangeTag::Delete => {
                // Present in 'Expected' (Old) but missing in 'Found' (New)
                let e_idx = change.old_index().unwrap();
                let e_col = &expected_cols[e_idx];

                writeln!(
                    report,
                    "- {:<20} | {:<15} | {:<15} | MISSING FROM DATABASE",
                    e_col.column_name().unwrap_or("-"),
                    e_col.type_name(),
                    "---"
                )
                .unwrap();
            }
            ChangeTag::Insert => {
                // Present in 'Found' (New) but not in 'Expected' (Old)
                let f_idx = change.new_index().unwrap();
                let f_col = &found_cols[f_idx];

                writeln!(
                    report,
                    "+ {:<20} | {:<15} | {:<15} | UNEXPECTED EXTRA COLUMN",
                    f_col.name(),
                    f_col.type_().name(),
                    "---"
                )
                .unwrap();
            }
        }
    }

    report
}

/// A trait that allows mapping rows from [tokio-postgres](<https://docs.rs/tokio-postgres>), to other types.
pub trait FromRow: Sized {
    /// The number of columns this type will attempt to consume
    const COLUMN_COUNT: usize;

    /// Try's to perform the conversion.
    ///
    /// Will return an error if the row does not contain the expected column names.
    ///
    /// May join the current row into the last one in which case None will be returned
    ///
    /// If last is none then this must never return None
    fn try_from_row_joined(
        last: Option<&mut Self>,
        row: &tokio_postgres::Row,
        index: usize,
    ) -> Result<Option<Self>, tokio_postgres::Error>;

    fn report_expected_columns() -> ExpectedColumns;
    fn try_assert_matches(columns: &[tokio_postgres::Column]) -> Result<(), ()>;

    /// Verifies that the column names and count match what is expected, panics on error
    ///
    /// The panic message is a detailed description of what is missing
    ///
    /// Do not override this implementation, instead implement report_expected_columns and try_assert_matches
    ///
    /// FromRow gets the columns by index, not by name, but it can still assert that the names match what is expected with this function
    ///
    /// This makes it possible to make queries where multiple columns have the same name, and still use the result with a flattened FromRow struct that matches the different column names
    fn assert_matches(columns: &[tokio_postgres::Column]) {
        if Self::try_assert_matches(columns).is_err() {
            std::panic::panic_any(report_expected_columns_mismatch(
                columns,
                &Self::report_expected_columns(),
            ))
        }
    }

    /// Perform the conversion.
    ///
    /// # Panics
    ///
    /// Panics if the row does not contain the expected column names.
    fn from_row(row: impl AsRow) -> Self {
        let row = row.as_row();
        Self::assert_matches(row.columns());
        Self::try_from_row(row).expect("could not convert column")
    }

    /// Try's to perform the conversion.
    ///
    /// Will return an error if the row does not contain the expected column names.
    fn try_from_row(row: impl AsRow) -> Result<Self, tokio_postgres::Error> {
        let row = row.as_row();
        Self::assert_matches(row.columns());
        Self::try_from_row_joined(None, row, 0).map(|x| {
            x.expect(
                "when try_from_row_joined is called with last = None it should never return None",
            )
        })
    }

    /// Perform the conversion on a slice of rows.
    ///
    /// # Panics
    ///
    /// Panics if the row does not contain the expected column names.
    fn from_slice(rows: &[tokio_postgres::Row]) -> Vec<Self> {
        let [first, ..] = rows else {
            return Vec::new();
        };
        Self::assert_matches(first.columns());
        let mut vec = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(this) =
                Self::try_from_row_joined(vec.last_mut(), row, 0).expect("could not convert column")
            {
                vec.push(this);
            }
        }
        vec
    }

    /// Try's to perform the conversion on a slice of rows.
    ///
    /// Will return an error if the row does not contain the expected column names.
    fn try_from_slice(rows: &[tokio_postgres::Row]) -> Result<Vec<Self>, tokio_postgres::Error> {
        let [first, ..] = rows else {
            return Ok(Vec::new());
        };
        Self::assert_matches(first.columns());
        let mut vec = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(this) = Self::try_from_row_joined(vec.last_mut(), row, 0)? {
                vec.push(this);
            }
        }
        Ok(vec)
    }

    /// Perform the conversion on a slice of rows.
    ///
    /// # Panics
    ///
    /// Panics if the row does not contain the expected column names.
    fn from_rows(rows: Vec<tokio_postgres::Row>) -> Vec<Self> {
        Self::from_slice(&rows)
        // vec_map::VecMapEx::map(rows, Self::from_row)
    }

    /// Try's to perform the conversion on a slice of rows.
    ///
    /// Will return an error if the row does not contain the expected column names.
    fn try_from_rows(rows: Vec<tokio_postgres::Row>) -> Result<Vec<Self>, tokio_postgres::Error> {
        Self::try_from_slice(&rows)
        // vec_map::VecMapEx::try_map(rows, Self::try_from_row)
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
    const COLUMN_COUNT: usize = T::COLUMN_COUNT;
    fn try_from_row_joined(
        mut last: Option<&mut Self>,
        row: &tokio_postgres::Row,
        index: usize,
    ) -> Result<Option<Self>, tokio_postgres::Error> {
        let this: Self = match T::try_from_row_joined(
            last.as_deref_mut().and_then(|l| l.as_mut()),
            row,
            index,
        ) {
            Ok(None) => return Ok(None),
            Ok(Some(row)) => Some(row),
            Err(e) if is_was_null(&e) => None,
            Err(error) => return Err(error),
        };
        Ok(Some(this))
    }
    fn report_expected_columns() -> ExpectedColumns {
        let mut columns = T::report_expected_columns().into_owned();
        for column in &mut columns {
            column.set_nullable();
        }
        columns.into()
    }
    fn try_assert_matches(columns: &[tokio_postgres::Column]) -> Result<(), ()> {
        T::try_assert_matches(columns)
    }
}

impl<T: FromRow> FromRow for Vec<T> {
    const COLUMN_COUNT: usize = T::COLUMN_COUNT;
    fn assert_matches(column: &[tokio_postgres::Column]) {
        T::assert_matches(column);
    }
    fn try_from_row_joined(
        last: Option<&mut Self>,
        row: &tokio_postgres::Row,
        index: usize,
    ) -> Result<Option<Self>, tokio_postgres::Error> {
        let Some(vec) = last else {
            match T::try_from_row_joined(None, row, index) {
                Ok(option) => return Ok(Some(vec![option.expect("when try_from_row_joined is called with last = None it should never return None")])),
                Err(e) if is_was_null(&e) => return Ok(Some(Vec::new())),
                Err(e) => return Err(e),
            }
        };
        if let Some(item) = T::try_from_row_joined(vec.last_mut(), row, index).or_else(|e| {
            if is_was_null(&e) {
                Ok(None)
            } else {
                Err(e)
            }
        })? {
            vec.push(item);
        }
        Ok(None)
    }
    fn report_expected_columns() -> ExpectedColumns {
        let mut columns = T::report_expected_columns().into_owned();
        for column in &mut columns {
            column.nullable = |_| true;
        }
        columns.into()
    }
    fn try_assert_matches(columns: &[tokio_postgres::Column]) -> Result<(), ()> {
        T::try_assert_matches(columns)
    }
}

fn is_was_null(e: &tokio_postgres::Error) -> bool {
    std::error::Error::source(&e)
        .is_some_and(|x| x.downcast_ref::<tokio_postgres::types::WasNull>().is_some())
}
