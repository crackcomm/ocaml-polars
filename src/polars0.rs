// Copyright © 2023-2024 Łukasz Kurowski. All rights reserved.
// SPDX-License-Identifier: MIT

pub use std::{
    convert::TryFrom,
    fmt,
    ops::{Deref, DerefMut},
};

pub mod pl {
    pub use polars::{lazy::dsl::WindowMapping, prelude::*, series::IsSorted};
    pub use polars_plan::dsl::{max_horizontal, min_horizontal, sum_horizontal};
    pub use polars_plan::prelude::{ApplyOptions, FunctionOptions, WindowType};
}

pub use ocaml::{bigarray, Error, FromValue, Pointer, ToValue};

#[macro_export]
macro_rules! deref {
    ($name:ident) => {
        $name.as_ref().deref()
    };
}

#[macro_export]
macro_rules! deref_mut {
    ($name:ident) => {
        $name.as_mut().deref_mut()
    };
}

macro_rules! make_custom {
    ($c:ident, $r:path, $p:ident) => {
        pub struct $c(pub $r);

        impl Deref for $c {
            type Target = $r;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl DerefMut for $c {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl From<$r> for $c {
            fn from(value: $r) -> Self {
                $c(value)
            }
        }

        ocaml::custom!($c);

        pub type $p = ocaml::Pointer<$c>;
    };
}

make_custom!(Series, pl::Series, SeriesPtr);
make_custom!(DataFrame, pl::DataFrame, DataFramePtr);
make_custom!(LazyFrame, pl::LazyFrame, LazyFramePtr);

#[derive(Debug)]
pub struct ErrorWithDesc {
    pub desc: String,
    pub error: Box<dyn std::error::Error>,
}

pub fn error_with_desc<E: std::error::Error + 'static>(error: E, desc: String) -> Error {
    Error::Error(Box::new(ErrorWithDesc {
        error: Box::new(error),
        desc,
    }))
}

impl fmt::Display for ErrorWithDesc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.desc, self.error)
    }
}

impl std::error::Error for ErrorWithDesc {}

#[derive(Debug)]
struct ErrorMessage(String);

impl std::error::Error for ErrorMessage {}

impl std::fmt::Display for ErrorMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.0.fmt(f)
    }
}

pub fn error_msg<S: ToString>(s: S) -> Error {
    Error::Error(Box::new(ErrorMessage(s.to_string())))
}
