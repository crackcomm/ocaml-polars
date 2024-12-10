// Copyright © 2023-2024 Łukasz Kurowski. All rights reserved.
// SPDX-License-Identifier: MIT

use std::sync::Arc;

pub use crate::polars0::*;
use ocamlrep_derive::ToOcamlRep;
use polars::prelude::Literal;

#[derive(Debug, Copy, Clone, FromValue, ToValue, ToOcamlRep)]
#[rust_to_ocaml(attr = "deriving compare, equal, sexp")]
pub enum TimeUnit {
    Nanoseconds,
    Microseconds,
    Milliseconds,
}

impl From<&TimeUnit> for pl::TimeUnit {
    fn from(value: &TimeUnit) -> Self {
        match value {
            TimeUnit::Nanoseconds => pl::TimeUnit::Nanoseconds,
            TimeUnit::Microseconds => pl::TimeUnit::Microseconds,
            TimeUnit::Milliseconds => pl::TimeUnit::Milliseconds,
        }
    }
}

impl From<pl::TimeUnit> for TimeUnit {
    fn from(value: pl::TimeUnit) -> Self {
        match value {
            pl::TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
            pl::TimeUnit::Microseconds => TimeUnit::Microseconds,
            pl::TimeUnit::Milliseconds => TimeUnit::Milliseconds,
        }
    }
}

#[derive(Debug, FromValue, ToValue, ToOcamlRep)]
#[rust_to_ocaml(attr = "deriving compare, equal, sexp")]
pub enum AnyValue {
    Int64(isize),
    Float32(f64), // always f64 on the ocaml side
    Float64(f64),
    Boolean(bool),
    Datetime(isize, TimeUnit),
}

impl TryFrom<pl::AnyValue<'_>> for AnyValue {
    type Error = Error;

    fn try_from(value: pl::AnyValue) -> Result<Self, Self::Error> {
        match value {
            pl::AnyValue::Int64(v) => Ok(AnyValue::Int64(v as isize)),
            pl::AnyValue::Float32(v) => Ok(AnyValue::Float32(v as f64)),
            pl::AnyValue::Float64(v) => Ok(AnyValue::Float64(v)),
            pl::AnyValue::Boolean(v) => Ok(AnyValue::Boolean(v)),
            pl::AnyValue::Datetime(v, unit, _) => Ok(AnyValue::Datetime(v as isize, unit.into())),
            dtype => Err(error_msg(format!(
                "AnyValue for dtype {:?} not implemented",
                dtype
            ))),
        }
    }
}

#[derive(Debug, Copy, Clone, FromValue, ToValue, ToOcamlRep)]
#[rust_to_ocaml(attr = "deriving compare, equal, sexp")]
pub enum DataType {
    Int64,
    Float32,
    Float64,
    Boolean,
    Datetime(TimeUnit),
}

impl From<&DataType> for pl::DataType {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Int64 => pl::DataType::Int64,
            DataType::Float32 => pl::DataType::Float32,
            DataType::Float64 => pl::DataType::Float64,
            DataType::Boolean => pl::DataType::Boolean,
            DataType::Datetime(tu) => pl::DataType::Datetime(tu.into(), None),
        }
    }
}

impl TryFrom<&pl::DataType> for DataType {
    type Error = Error;

    fn try_from(value: &pl::DataType) -> Result<Self, Self::Error> {
        match value {
            pl::DataType::Int64 => Ok(DataType::Int64),
            pl::DataType::Float32 => Ok(DataType::Float32),
            pl::DataType::Float64 => Ok(DataType::Float64),
            pl::DataType::Boolean => Ok(DataType::Boolean),
            pl::DataType::Datetime(tu, _) => Ok(DataType::Datetime(TimeUnit::from(*tu))),
            _ => Err(Error::Message("dtype not supported")),
        }
    }
}

#[derive(FromValue, ToOcamlRep)]
#[rust_to_ocaml(attr = "deriving compare, equal, sexp")]
pub enum Duration {
    Slots(isize),
    Duration(String),
}

impl From<&Duration> for pl::Duration {
    fn from(value: &Duration) -> Self {
        match value {
            Duration::Slots(s) => pl::Duration::new(*s as i64),
            Duration::Duration(s) => pl::Duration::parse(s),
        }
    }
}

#[derive(FromValue, ToOcamlRep)]
pub enum IsSorted {
    Ascending,
    Descending,
    #[rust_to_ocaml(name = "Not_sorted")]
    Not,
}

impl From<&IsSorted> for pl::IsSorted {
    fn from(value: &IsSorted) -> Self {
        match value {
            IsSorted::Ascending => pl::IsSorted::Ascending,
            IsSorted::Descending => pl::IsSorted::Descending,
            IsSorted::Not => pl::IsSorted::Not,
        }
    }
}

#[derive(Debug, FromValue, ToValue)]
pub enum Comparison {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl Into<pl::Operator> for Comparison {
    fn into(self) -> pl::Operator {
        match self {
            Comparison::Eq => pl::Operator::Eq,
            Comparison::NotEq => pl::Operator::NotEq,
            Comparison::Lt => pl::Operator::Lt,
            Comparison::LtEq => pl::Operator::LtEq,
            Comparison::Gt => pl::Operator::Gt,
            Comparison::GtEq => pl::Operator::GtEq,
        }
    }
}

impl Into<pl::Expr> for AnyValue {
    fn into(self) -> pl::Expr {
        match self {
            AnyValue::Int64(v) => (v as i64).lit(),
            AnyValue::Float32(v) => v.lit(),
            AnyValue::Float64(v) => v.lit(),
            AnyValue::Boolean(v) => v.lit(),
            AnyValue::Datetime(v, unit) => {
                pl::LiteralValue::DateTime(v as i64, pl::TimeUnit::from(&unit), None).lit()
            }
        }
    }
}

#[derive(Debug, FromValue, ToValue, ToOcamlRep)]
pub enum Operator {
    Eq,
    EqValidity,
    NotEq,
    NotEqValidity,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
    TrueDivide,
    FloorDivide,
    Modulus,
    And,
    Or,
    Xor,
}

impl From<&Operator> for pl::Operator {
    fn from(value: &Operator) -> Self {
        match value {
            Operator::Eq => pl::Operator::Eq,
            Operator::EqValidity => pl::Operator::EqValidity,
            Operator::NotEq => pl::Operator::NotEq,
            Operator::NotEqValidity => pl::Operator::NotEqValidity,
            Operator::Lt => pl::Operator::Lt,
            Operator::LtEq => pl::Operator::LtEq,
            Operator::Gt => pl::Operator::Gt,
            Operator::GtEq => pl::Operator::GtEq,
            Operator::Plus => pl::Operator::Plus,
            Operator::Minus => pl::Operator::Minus,
            Operator::Multiply => pl::Operator::Multiply,
            Operator::Divide => pl::Operator::Divide,
            Operator::TrueDivide => pl::Operator::TrueDivide,
            Operator::FloorDivide => pl::Operator::FloorDivide,
            Operator::Modulus => pl::Operator::Modulus,
            Operator::And => pl::Operator::And,
            Operator::Or => pl::Operator::Or,
            Operator::Xor => pl::Operator::Xor,
        }
    }
}

#[derive(FromValue)]
pub enum LiteralValue {
    Null,
    Boolean(bool),
    String(String),
    // Binary(Vec<u8, Global>),
    // UInt32(u32),
    UInt64(usize),
    // Int32(i32),
    Int64(isize),
    Float32(f64),
    Float64(f64),
    Range {
        low: isize,
        high: isize,
        data_type: DataType,
    },
    Series(SeriesPtr),
}

impl ocamlrep::ToOcamlRep for LiteralValue {
    fn to_ocamlrep<'a, A: ocamlrep::Allocator>(&'a self, _alloc: &'a A) -> ocamlrep::Value<'a> {
        todo!()
    }
}

impl From<&LiteralValue> for pl::LiteralValue {
    fn from(value: &LiteralValue) -> Self {
        match value {
            LiteralValue::Null => pl::LiteralValue::Null,
            LiteralValue::Boolean(b) => pl::LiteralValue::Boolean(*b),
            LiteralValue::String(s) => pl::LiteralValue::String(s.clone()),
            LiteralValue::UInt64(u) => pl::LiteralValue::UInt64(*u as u64),
            LiteralValue::Int64(i) => pl::LiteralValue::Int64(*i as i64),
            LiteralValue::Float32(f) => pl::LiteralValue::Float32(*f as f32),
            LiteralValue::Float64(f) => pl::LiteralValue::Float64(*f),
            LiteralValue::Range {
                low,
                high,
                data_type,
            } => pl::LiteralValue::Range {
                low: *low as i64,
                high: *high as i64,
                data_type: pl::DataType::from(data_type),
            },
            LiteralValue::Series(series) => {
                pl::LiteralValue::Series(pl::SpecialEq::new(series.as_ref().0.clone()))
            }
        }
    }
}

#[derive(ToValue, FromValue, ToOcamlRep)]
pub enum WindowMapping {
    GroupsToRows,
    Explode,
    Join,
}

impl From<&WindowMapping> for pl::WindowMapping {
    fn from(value: &WindowMapping) -> Self {
        match value {
            WindowMapping::GroupsToRows => pl::WindowMapping::GroupsToRows,
            WindowMapping::Explode => pl::WindowMapping::Explode,
            WindowMapping::Join => pl::WindowMapping::Join,
        }
    }
}

#[derive(FromValue, ToOcamlRep)]
pub enum BooleanFunction {
    All { ignore_nulls: bool },
    Any { ignore_nulls: bool },
    Not,
    IsNull,
    IsNotNull,
    IsFinite,
    IsInfinite,
    IsNan,
    IsNotNan,
    // #[cfg(feature = "is_first")]
    // IsFirst,
    // #[cfg(feature = "is_unique")]
    // IsUnique,
    // #[cfg(feature = "is_unique")]
    // IsDuplicated,
    // #[cfg(feature = "is_in")]
    // IsIn,
    AllHorizontal,
    AnyHorizontal,
}

impl From<&BooleanFunction> for pl::BooleanFunction {
    fn from(value: &BooleanFunction) -> Self {
        match value {
            BooleanFunction::All { ignore_nulls } => pl::BooleanFunction::All {
                ignore_nulls: *ignore_nulls,
            },
            BooleanFunction::Any { ignore_nulls } => pl::BooleanFunction::Any {
                ignore_nulls: *ignore_nulls,
            },
            BooleanFunction::Not => pl::BooleanFunction::Not,
            BooleanFunction::IsNull => pl::BooleanFunction::IsNull,
            BooleanFunction::IsNotNull => pl::BooleanFunction::IsNotNull,
            BooleanFunction::IsFinite => pl::BooleanFunction::IsFinite,
            BooleanFunction::IsInfinite => pl::BooleanFunction::IsInfinite,
            BooleanFunction::IsNan => pl::BooleanFunction::IsNan,
            BooleanFunction::IsNotNan => pl::BooleanFunction::IsNotNan,
            BooleanFunction::AllHorizontal => pl::BooleanFunction::AllHorizontal,
            BooleanFunction::AnyHorizontal => pl::BooleanFunction::AnyHorizontal,
        }
    }
}

#[derive(FromValue, ToOcamlRep)]
pub enum FunctionExpr {
    Abs,
    NullCount,
    // Pow(PowFunction),
    // StringExpr(StringFunction),
    // BinaryExpr(BinaryFunction),
    // TODO:
    // TemporalExpr(TemporalFunction),
    // Trigonometry(TrigonometricFunction),
    // Atan2,
    FillNull,
    // ShiftAndFill {
    //     periods: isize,
    // },
    DropNans,
    // Clip {
    //     min: Option<pl::AnyValue<'static>>,
    //     max: Option<pl::AnyValue<'static>>,
    // },
    // ListExpr(ListFunction),
    Shift,
    CumCount { reverse: bool },
    CumSum { reverse: bool },
    CumProd { reverse: bool },
    CumMin { reverse: bool },
    CumMax { reverse: bool },
    Reverse,
    Boolean(BooleanFunction),
    Coalesce,
    ShrinkType,
    Entropy { base: f64, normalize: bool },
    Log { base: f64 },
    Log1p,
    Exp,
    Unique(bool),
    Round { decimals: u32 },
    Floor,
    Ceil,
    UpperBound,
    LowerBound,
    ConcatExpr(bool),
    // Correlation { method: CorrelationMethod, ddof: u8 },
    ToPhysical,
    SetSortedFlag(IsSorted),
}

impl From<&FunctionExpr> for pl::FunctionExpr {
    fn from(value: &FunctionExpr) -> Self {
        match value {
            FunctionExpr::Abs => pl::FunctionExpr::Abs,
            FunctionExpr::NullCount => pl::FunctionExpr::NullCount,
            FunctionExpr::FillNull => pl::FunctionExpr::FillNull {
                super_type: pl::DataType::Unknown,
            },
            FunctionExpr::DropNans => pl::FunctionExpr::DropNans,
            FunctionExpr::Shift => pl::FunctionExpr::Shift,
            FunctionExpr::CumCount { reverse } => pl::FunctionExpr::CumCount { reverse: *reverse },
            FunctionExpr::CumSum { reverse } => pl::FunctionExpr::CumSum { reverse: *reverse },
            FunctionExpr::CumProd { reverse } => pl::FunctionExpr::CumProd { reverse: *reverse },
            FunctionExpr::CumMin { reverse } => pl::FunctionExpr::CumMin { reverse: *reverse },
            FunctionExpr::CumMax { reverse } => pl::FunctionExpr::CumMax { reverse: *reverse },
            FunctionExpr::Reverse => pl::FunctionExpr::Reverse,
            FunctionExpr::Boolean(ocaml_bool_func) => {
                pl::FunctionExpr::Boolean(ocaml_bool_func.into())
            }
            FunctionExpr::Coalesce => pl::FunctionExpr::Coalesce,
            FunctionExpr::ShrinkType => pl::FunctionExpr::ShrinkType,
            FunctionExpr::Entropy { base, normalize } => pl::FunctionExpr::Entropy {
                base: *base,
                normalize: *normalize,
            },
            FunctionExpr::Log { base } => pl::FunctionExpr::Log { base: *base },
            FunctionExpr::Log1p => pl::FunctionExpr::Log1p,
            FunctionExpr::Exp => pl::FunctionExpr::Exp,
            FunctionExpr::Unique(unique) => pl::FunctionExpr::Unique(*unique),
            FunctionExpr::Round { decimals } => pl::FunctionExpr::Round {
                decimals: *decimals,
            },
            FunctionExpr::Floor => pl::FunctionExpr::Floor,
            FunctionExpr::Ceil => pl::FunctionExpr::Ceil,
            FunctionExpr::UpperBound => pl::FunctionExpr::UpperBound,
            FunctionExpr::LowerBound => pl::FunctionExpr::LowerBound,
            FunctionExpr::ConcatExpr(concat) => pl::FunctionExpr::ConcatExpr(*concat),
            FunctionExpr::ToPhysical => pl::FunctionExpr::ToPhysical,
            FunctionExpr::SetSortedFlag(ocaml_is_sorted) => {
                pl::FunctionExpr::SetSortedFlag(ocaml_is_sorted.into())
            }
        }
    }
}

#[derive(FromValue, ToOcamlRep)]
pub enum ApplyOptions {
    GroupWise,
    ApplyList,
    ElementWise,
}

impl From<&ApplyOptions> for pl::ApplyOptions {
    fn from(value: &ApplyOptions) -> Self {
        match value {
            ApplyOptions::GroupWise => pl::ApplyOptions::GroupWise,
            ApplyOptions::ApplyList => pl::ApplyOptions::ApplyList,
            ApplyOptions::ElementWise => pl::ApplyOptions::ElementWise,
        }
    }
}

#[derive(FromValue, ToOcamlRep)]
pub struct FunctionOptions {
    pub collect_groups: ApplyOptions,
    pub cast_to_supertypes: bool,
}

impl From<&FunctionOptions> for pl::FunctionOptions {
    fn from(value: &FunctionOptions) -> Self {
        pl::FunctionOptions {
            collect_groups: pl::ApplyOptions::from(&value.collect_groups),
            cast_to_supertypes: value.cast_to_supertypes,
            ..Default::default()
        }
    }
}

#[derive(FromValue, ToOcamlRep)]
pub struct SortOptions {
    pub descending: bool,
    pub nulls_last: bool,
    pub multithreaded: bool,
    pub maintain_order: bool,
}

impl From<&SortOptions> for pl::SortOptions {
    fn from(value: &SortOptions) -> Self {
        pl::SortOptions {
            descending: value.descending,
            nulls_last: value.nulls_last,
            multithreaded: value.multithreaded,
            maintain_order: value.maintain_order,
        }
    }
}

#[derive(FromValue, ToOcamlRep)]
pub struct SortMultipleOptions {
    pub descending: Vec<bool>,
    pub nulls_last: bool,
    pub multithreaded: bool,
    pub maintain_order: bool,
}

impl From<&SortMultipleOptions> for pl::SortMultipleOptions {
    fn from(value: &SortMultipleOptions) -> Self {
        pl::SortMultipleOptions {
            descending: value.descending.clone(),
            nulls_last: value.nulls_last,
            multithreaded: value.multithreaded,
            maintain_order: value.maintain_order,
        }
    }
}

#[derive(FromValue, ToOcamlRep)]
pub enum ClosedWindow {
    Left,
    Right,
    Both,
    None,
}

impl From<&ClosedWindow> for pl::ClosedWindow {
    fn from(value: &ClosedWindow) -> Self {
        match value {
            ClosedWindow::Left => pl::ClosedWindow::Left,
            ClosedWindow::Right => pl::ClosedWindow::Right,
            ClosedWindow::Both => pl::ClosedWindow::Both,
            ClosedWindow::None => pl::ClosedWindow::None,
        }
    }
}

#[derive(FromValue, ToOcamlRep)]
pub enum Rolling {
    Min,
    Max,
    Mean,
    Sum,
    Median,
    // TODO: QuantileInterpolOptions
    Quantile(f64),
    Var,
    Std,
}

#[derive(FromValue, ToOcamlRep)]
pub struct RollingOptions {
    pub window_size: Duration,
    pub min_periods: usize,
    pub weights: Option<Vec<f64>>,
    pub center: bool,
    pub by: Option<String>,
    pub closed_window: Option<ClosedWindow>,
}

impl From<&RollingOptions> for pl::RollingOptions {
    fn from(value: &RollingOptions) -> Self {
        pl::RollingOptions {
            window_size: pl::Duration::from(&value.window_size),
            min_periods: value.min_periods,
            weights: value.weights.clone(),
            center: value.center,
            by: value.by.clone(),
            closed_window: value.closed_window.as_ref().map(pl::ClosedWindow::from),
            ..Default::default()
        }
    }
}

#[derive(FromValue, ToOcamlRep)]
pub enum Horizontal {
    Min,
    Max,
    Sum,
}

#[derive(FromValue, ToOcamlRep)]
pub enum Expr {
    Alias(Box<Expr>, String),
    Column(String),
    Columns(Vec<String>),
    DtypeColumn(Vec<DataType>),
    Literal(LiteralValue),
    BinaryExpr {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
        strict: bool,
    },
    Sort {
        expr: Box<Expr>,
        options: SortOptions,
    },
    Gather {
        expr: Box<Expr>,
        idx: Box<Expr>,
        returns_scalar: bool,
    },
    SortBy {
        expr: Box<Expr>,
        by: Vec<Expr>,
        sort_options: SortMultipleOptions,
    },
    Agg(AggExpr),
    Ternary {
        predicate: Box<Expr>,
        truthy: Box<Expr>,
        falsy: Box<Expr>,
    },
    Function {
        input: Vec<Expr>,
        function: FunctionExpr,
        options: FunctionOptions,
    },
    Explode(Box<Expr>),
    Filter {
        input: Box<Expr>,
        by: Box<Expr>,
    },
    Window {
        function: Box<Expr>,
        partition_by: Vec<Expr>,
        // order_by: Option<Box<Expr>>,
        options: WindowMapping,
    },
    Wildcard,
    Slice {
        input: Box<Expr>,
        offset: Box<Expr>,
        length: Box<Expr>,
    },
    // Exclude(Box<Expr>, Vec<Excluded>),
    KeepName(Box<Expr>),
    Len,
    Nth(isize),
    // NOT SUPPORTED:
    // RenameAlias {
    //     function: SpecialEq<Arc<dyn RenameAliasFn>>,
    //     expr: Arc<Expr>,
    // },
    // AnonymousFunction {
    //     input: Vec<Expr>,
    //     function: SpecialEq<Arc<dyn SeriesUdf>>,
    //     output_type: SpecialEq<Arc<dyn FunctionOutputField>>,
    //     options: pl::FunctionOptions,
    // },
    // Selector(Selector),
    Rolling(Box<Expr>, Rolling, RollingOptions),
    Horizontal {
        input: Vec<Box<Expr>>,
        op: Horizontal,
    },
    ForwardFill(Box<Expr>, Option<u32>),
}

fn arc_expr(e: &Box<Expr>) -> Arc<pl::Expr> {
    Arc::new(e.deref().into())
}

impl From<&Expr> for pl::Expr {
    fn from(value: &Expr) -> Self {
        match value {
            Expr::Alias(expr, name) => pl::Expr::Alias(arc_expr(expr), name.to_owned().into()),
            Expr::Column(column) => pl::Expr::Column(column.to_owned().into()),
            Expr::Columns(columns) => pl::Expr::Columns(columns.to_vec()),
            Expr::DtypeColumn(data_types) => {
                pl::Expr::DtypeColumn(data_types.into_iter().map(From::from).collect())
            }
            Expr::Literal(literal) => pl::Expr::Literal(literal.into()),
            Expr::BinaryExpr { left, op, right } => pl::Expr::BinaryExpr {
                left: arc_expr(left),
                op: op.into(),
                right: arc_expr(right),
            },
            Expr::Cast {
                expr,
                data_type,
                strict,
            } => pl::Expr::Cast {
                expr: arc_expr(expr),
                data_type: data_type.into(),
                strict: *strict,
            },
            Expr::Sort { expr, options } => pl::Expr::Sort {
                expr: arc_expr(expr),
                options: options.into(),
            },
            Expr::Gather {
                expr,
                idx,
                returns_scalar,
            } => pl::Expr::Gather {
                expr: arc_expr(expr),
                idx: arc_expr(idx),
                returns_scalar: *returns_scalar,
            },
            Expr::SortBy {
                expr,
                by,
                sort_options,
            } => pl::Expr::SortBy {
                expr: arc_expr(expr),
                by: by.into_iter().map(From::from).collect(),
                sort_options: sort_options.into(),
            },
            Expr::Agg(agg_expr) => pl::Expr::Agg(agg_expr.into()),
            Expr::Ternary {
                predicate,
                truthy,
                falsy,
            } => pl::Expr::Ternary {
                predicate: arc_expr(predicate),
                truthy: arc_expr(truthy),
                falsy: arc_expr(falsy),
            },
            Expr::Function {
                input,
                function,
                options,
            } => pl::Expr::Function {
                input: input.into_iter().map(From::from).collect(),
                function: function.into(),
                options: options.into(),
            },
            Expr::Explode(expr) => pl::Expr::Explode(arc_expr(expr)),
            Expr::Filter { input, by } => pl::Expr::Filter {
                input: arc_expr(input),
                by: arc_expr(by),
            },
            Expr::Window {
                function,
                partition_by,
                // order_by,
                options,
            } => pl::Expr::Window {
                function: arc_expr(function),
                partition_by: partition_by.into_iter().map(From::from).collect(),
                // order_by: order_by.as_ref().map(arc_expr),
                options: pl::WindowType::Over(options.into()),
            },
            Expr::Wildcard => pl::Expr::Wildcard,
            Expr::Slice {
                input,
                offset,
                length,
            } => pl::Expr::Slice {
                input: arc_expr(input),
                offset: arc_expr(offset),
                length: arc_expr(length),
            },
            Expr::KeepName(expr) => pl::Expr::KeepName(arc_expr(expr)),
            Expr::Len => pl::Expr::Len,
            Expr::Nth(index) => pl::Expr::Nth(*index as i64),
            Expr::Rolling(expr, op, opts) => {
                let expr = pl::Expr::from(expr.deref());
                match op {
                    Rolling::Min => expr.rolling_min(opts.into()),
                    Rolling::Max => expr.rolling_max(opts.into()),
                    Rolling::Mean => expr.rolling_mean(opts.into()),
                    Rolling::Sum => expr.rolling_sum(opts.into()),
                    Rolling::Median => expr.rolling_median(opts.into()),
                    Rolling::Quantile(quantile) => expr.rolling_quantile(
                        pl::QuantileInterpolOptions::Nearest,
                        *quantile,
                        opts.into(),
                    ),
                    Rolling::Var => expr.rolling_var(opts.into()),
                    Rolling::Std => expr.rolling_std(opts.into()),
                }
            }
            Expr::Horizontal { input, op } => {
                let exprs: Vec<pl::Expr> = input.into_iter().map(|e| e.deref().into()).collect();
                match op {
                    Horizontal::Min => pl::min_horizontal(exprs).expect("min_horizontal"),
                    Horizontal::Max => pl::max_horizontal(exprs).expect("max_horizontal"),
                    Horizontal::Sum => pl::sum_horizontal(exprs).expect("sum_horizontal"),
                }
            }
            Expr::ForwardFill(expr, limit) => pl::Expr::from(expr.deref()).forward_fill(*limit),
        }
    }
}

#[derive(FromValue, ToOcamlRep)]
#[rust_to_ocaml(and)]
pub enum AggExpr {
    Min {
        input: Box<Expr>,
        propagate_nans: bool,
    },
    Max {
        input: Box<Expr>,
        propagate_nans: bool,
    },
    Median(Box<Expr>),
    #[rust_to_ocaml(name = "N_unique")]
    NUnique(Box<Expr>),
    First(Box<Expr>),
    Last(Box<Expr>),
    Mean(Box<Expr>),
    Implode(Box<Expr>),
    // include_nulls
    Count(Box<Expr>, bool),
    // Quantile {
    //     expr: Box<Expr>,
    //     quantile: Box<Expr>,
    //     interpol: QuantileInterpolOptions,
    // },
    Sum(Box<Expr>),
    AggGroups(Box<Expr>),
    Std(Box<Expr>, isize),
    Var(Box<Expr>, isize),
}

impl From<&AggExpr> for pl::AggExpr {
    fn from(ocaml_agg_expr: &AggExpr) -> Self {
        match ocaml_agg_expr {
            AggExpr::Min {
                input,
                propagate_nans,
            } => pl::AggExpr::Min {
                input: arc_expr(input),
                propagate_nans: *propagate_nans,
            },
            AggExpr::Max {
                input,
                propagate_nans,
            } => pl::AggExpr::Max {
                input: arc_expr(input),
                propagate_nans: *propagate_nans,
            },
            AggExpr::Median(expr) => pl::AggExpr::Median(arc_expr(expr)),
            AggExpr::NUnique(expr) => pl::AggExpr::NUnique(arc_expr(expr)),
            AggExpr::First(expr) => pl::AggExpr::First(arc_expr(expr)),
            AggExpr::Last(expr) => pl::AggExpr::Last(arc_expr(expr)),
            AggExpr::Mean(expr) => pl::AggExpr::Mean(arc_expr(expr)),
            AggExpr::Implode(expr) => pl::AggExpr::Implode(arc_expr(expr)),
            AggExpr::Count(expr, include_nulls) => {
                pl::AggExpr::Count(arc_expr(expr), *include_nulls)
            }
            AggExpr::Sum(expr) => pl::AggExpr::Sum(arc_expr(expr)),
            AggExpr::AggGroups(expr) => pl::AggExpr::AggGroups(arc_expr(expr)),
            AggExpr::Std(expr, ddof) => pl::AggExpr::Std(arc_expr(expr), *ddof as u8),
            AggExpr::Var(expr, ddof) => pl::AggExpr::Var(arc_expr(expr), *ddof as u8),
        }
    }
}

#[derive(Debug, FromValue)]
pub struct ReadCsv {
    pub skip_rows: usize,
    pub has_header: bool,
    pub columns: Option<Vec<String>>,
    pub schema: Option<Vec<(String, DataType)>>,
    pub n_threads: usize,
}
