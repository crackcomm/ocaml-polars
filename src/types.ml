(* Copyright © 2024 Łukasz Kurowski. All rights reserved.
   SPDX-License-Identifier: MIT *)

open! Core

type time_unit =
  | Nanoseconds
  | Microseconds
  | Milliseconds
[@@deriving compare, equal, sexp]

type data_type =
  | Int64
  | Float32
  | Float64
  | Boolean
  | Datetime of time_unit
[@@deriving compare, equal, sexp]

type any_value =
  | Int64 of int
  | Float32 of float
  | Float64 of float
  | Boolean of bool
  | Datetime of int * time_unit
[@@deriving compare, equal, sexp]

type duration =
  | Slots of int
  | Duration of string
[@@deriving compare, equal, sexp]

type is_sorted =
  | Ascending
  | Descending
  | Not_sorted

type comparison =
  | Eq
  | NotEq
  | Lt
  | LtEq
  | Gt
  | GtEq

type operator =
  | Eq
  | EqValidity
  | NotEq
  | NotEqValidity
  | Lt
  | LtEq
  | Gt
  | GtEq
  | Plus
  | Minus
  | Multiply
  | Divide
  | TrueDivide
  | FloorDivide
  | Modulus
  | And
  | Or
  | Xor

type literal_value =
  | Null
  | Boolean of bool
  | String of string
  | UInt64 of int
  | Int64 of int
  | Float32 of float
  | Float64 of float
  | Range of
      { low : int
      ; high : int
      ; data_type : data_type
      }
  | Series of Polars0.series

type window_mapping =
  | GroupsToRows
  | Explode
  | Join

type boolean_function =
  | All of { ignore_nulls : bool }
  | Any of { ignore_nulls : bool }
  | Not
  | IsNull
  | IsNotNull
  | IsFinite
  | IsInfinite
  | IsNan
  | IsNotNan
  | AllHorizontal
  | AnyHorizontal

type function_expr =
  | Abs
  | NullCount
  | FillNull
  | DropNans
  | Shift
  | CumCount of { reverse : bool }
  | CumSum of { reverse : bool }
  | CumProd of { reverse : bool }
  | CumMin of { reverse : bool }
  | CumMax of { reverse : bool }
  | Reverse
  | Boolean of boolean_function
  | Coalesce
  | ShrinkType
  | Entropy of
      { base : float
      ; normalize : bool
      }
  | Log of { base : float }
  | Log1p
  | Exp
  | Unique of bool
  | Round of { decimals : int }
  | Floor
  | Ceil
  | UpperBound
  | LowerBound
  | ConcatExpr of bool
  | ToPhysical
  | SetSortedFlag of is_sorted

type apply_options =
  | GroupWise
  | ApplyList
  | ElementWise

type function_options =
  { collect_groups : apply_options
  ; cast_to_supertypes : bool
  }

type sort_options =
  { descending : bool
  ; nulls_last : bool
  ; multithreaded : bool
  ; maintain_order : bool
  }

type sort_multiple_options =
  { descending : bool array
  ; nulls_last : bool
  ; multithreaded : bool
  ; maintain_order : bool
  }

type closed_window =
  | Left
  | Right
  | Both
  | None

type rolling =
  | Min
  | Max
  | Mean
  | Sum
  | Median
  | Quantile of float
  | Var
  | Std

type rolling_options =
  { window_size : duration
  ; min_periods : int
  ; weights : float array option
  ; center : bool
  ; by : string option
  ; closed_window : closed_window option
  }

type horizontal =
  | Min
  | Max
  | Sum

type expr =
  | Alias of expr * string
  | Column of string
  | Columns of string array
  | DtypeColumn of data_type array
  | Literal of literal_value
  | BinaryExpr of
      { left : expr
      ; op : operator
      ; right : expr
      }
  | Cast of
      { expr : expr
      ; data_type : data_type
      ; strict : bool
      }
  | Sort of
      { expr : expr
      ; options : sort_options
      }
  | Gather of
      { expr : expr
      ; idx : expr
      ; returns_scalar : bool
      }
  | SortBy of
      { expr : expr
      ; by : expr array
      ; sort_options : sort_multiple_options
      }
  | Agg of agg_expr
  | Ternary of
      { predicate : expr
      ; truthy : expr
      ; falsy : expr
      }
  | Function of
      { input : expr array
      ; function_ : function_expr
      ; options : function_options
      }
  | Explode of expr
  | Filter of
      { input : expr
      ; by : expr
      }
  | Window of
      { function_ : expr
      ; partition_by : expr array
      ; options : window_mapping
      }
  | Wildcard
  | Slice of
      { input : expr
      ; offset : expr
      ; length : expr
      }
  | KeepName of expr
  | Len
  | Nth of int
  | Rolling of expr * rolling * rolling_options
  | Horizontal of
      { input : expr array
      ; op : horizontal
      }
  | ForwardFill of expr * int option

and agg_expr =
  | Min of
      { input : expr
      ; propagate_nans : bool
      }
  | Max of
      { input : expr
      ; propagate_nans : bool
      }
  | Median of expr
  | N_unique of expr
  | First of expr
  | Last of expr
  | Mean of expr
  | Implode of expr
  | Count of expr * bool
  | Sum of expr
  | AggGroups of expr
  | Std of expr * int
  | Var of expr * int

type read_csv =
  { skip_rows : int
  ; has_header : bool
  ; columns : string array option
  ; schema : (string * data_type) array option
  ; n_threads : int
  }
