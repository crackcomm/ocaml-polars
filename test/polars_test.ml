(* Copyright © 2023-2024 Łukasz Kurowski. All rights reserved.
   SPDX-License-Identifier: MIT *)

open Core
open Polars
open Bigarray
open Polars_testing

let%expect_test "polars dataframe" =
  let df = Polars_testdata.trades1 () in
  Polars.print df;
  [%expect
    {|
    shape: (223, 7)
    | trade_id   | price        | qty   | first_id   | last_id    | timestamp     | is_bid |
    | ---        | ---          | ---   | ---        | ---        | ---           | ---    |
    | i64        | f32          | f32   | i64        | i64        | i64           | bool   |
    |------------|--------------|-------|------------|------------|---------------|--------|
    | 1845263590 | 25899.900391 | 0.005 | 4071974096 | 4071974096 | 1694217604155 | false  |
    | 1845263591 | 25899.800781 | 1.02  | 4071974097 | 4071974098 | 1694217604162 | true   |
    | 1845263592 | 25899.900391 | 0.019 | 4071974099 | 4071974099 | 1694217604185 | false  |
    | 1845263593 | 25899.800781 | 0.086 | 4071974100 | 4071974101 | 1694217604189 | true   |
    | …          | …            | …     | …          | …          | …             | …      |
    | 1845263809 | 25900.099609 | 0.001 | 4071974565 | 4071974565 | 1694217660053 | false  |
    | 1845263810 | 25900.0      | 0.007 | 4071974566 | 4071974566 | 1694217660288 | true   |
    | 1845263811 | 25900.099609 | 0.004 | 4071974567 | 4071974567 | 1694217660344 | false  |
    | 1845263812 | 25900.099609 | 0.016 | 4071974568 | 4071974568 | 1694217660528 | false  | |}];
  (* column names *)
  print_s [%sexp (column_names df : string array)];
  [%expect {| (trade_id price qty first_id last_id timestamp is_bid) |}];
  (* shape *)
  print_s [%sexp (shape df : int * int)];
  [%expect {| (223 7) |}];
  (* equal to self *)
  print_s [%sexp (equal df df : bool)];
  [%expect {| true |}];
  (* sub and length *)
  let df' = sub ~pos:1 ~len:10 df in
  print_s [%sexp (length df' : int)];
  [%expect {| 10 |}];
  (* frame equal *)
  print_s [%sexp (equal df' (sub ~pos:1 ~len:10 df) : bool)];
  [%expect {| true |}];
  (* get_row *)
  print_s [%sexp (get_row_exn ~idx:1 df' : Any_value.t array)];
  [%expect
    {|
    ((Int64 1845263592) (Float32 25899.900390625) (Float32 0.018999999389052391)
     (Int64 4071974099) (Int64 4071974099) (Int64 1694217604185) (Boolean false)) |}];
  (* get *)
  print_s [%sexp (get_exn ~col:0 ~idx:1 df' : Any_value.t)];
  [%expect {| (Int64 1845263592) |}];
  print_s [%sexp (get_exn ~col:6 ~idx:1 df : Any_value.t)];
  [%expect {| (Boolean true) |}];
  print_s [%sexp (get_exn ~col:2 ~idx:1 df : Any_value.t)];
  [%expect {| (Float32 1.0199999809265137) |}];
  (* get_by_name *)
  print_s [%sexp (get_by_name_exn ~col:"qty" ~idx:1 df : Any_value.t)];
  [%expect {| (Float32 1.0199999809265137) |}];
  (* select_by_name *)
  let series = select_by_name_exn df ~col:"qty" in
  print_s [%sexp (series : Series.t)];
  [%expect {| (Series qty 223) |}];
  (* select_by_idx *)
  let series = select_by_idx_exn df ~col:2 in
  (* Series *)
  (* length *)
  print_s [%sexp (Series.length series : int)];
  [%expect {| 223 |}];
  print_s [%sexp (Series.get_exn ~idx:1 series : Any_value.t)];
  [%expect {| (Float32 1.0199999809265137) |}];
  (* write parquet *)
  let filepath = Filename_unix.temp_file "trades-1" ".parquet" in
  printf "%d\n" (write_parquet df filepath);
  [%expect {| 4619 |}];
  print_endline (Md5.digest_file_blocking filepath |> Md5.to_hex);
  [%expect {| a3e3b240e7b464e442682e9caad97ec8 |}];
  (* read parquet *)
  let df' = read_parquet filepath in
  print_s [%sexp (equal df df' : bool)];
  [%expect {| true |}]
;;

let%expect_test "sorting" =
  let df = Polars_testdata.trades1 () in
  let df' = sort df ~descending:true ~col:"timestamp" in
  print_s [%sexp (get_exn ~col:5 ~idx:0 df' : Any_value.t)];
  [%expect {| (Int64 1694217660528) |}];
  sort_in_place df' ~col:"timestamp";
  print_s [%sexp (get_exn ~col:5 ~idx:0 df' : Any_value.t)];
  [%expect {| (Int64 1694217604155) |}]
;;

let%expect_test "filter_col_by_name" =
  let df = Polars_testdata.trades1 () in
  let df =
    Polars.filter_col_by_name df ~col:"timestamp" Gt (Polars.Int64 1694217620883)
  in
  let df =
    Polars.filter_col_by_name df ~col:"timestamp" LtEq (Polars.Int64 1694217636694)
  in
  let last = Polars.length df - 1 in
  print_s [%sexp (get_by_name_exn ~col:"timestamp" ~idx:0 df : Any_value.t)];
  [%expect {| (Int64 1694217621185) |}];
  print_s [%sexp (get_by_name_exn ~col:"timestamp" ~idx:last df : Any_value.t)];
  [%expect {| (Int64 1694217636694) |}]
;;

let%expect_test "filter_col_by_name_multi" =
  let df = Polars_testdata.trades1 () in
  let df =
    Polars.filter_col_by_name_multi
      df
      [| "timestamp", Gt, Polars.Int64 1694217620883
       ; "timestamp", LtEq, Polars.Int64 1694217636694
      |]
  in
  let last = Polars.length df - 1 in
  print_s [%sexp (get_by_name_exn ~col:"timestamp" ~idx:0 df : Any_value.t)];
  [%expect {| (Int64 1694217621185) |}];
  print_s [%sexp (get_by_name_exn ~col:"timestamp" ~idx:last df : Any_value.t)];
  [%expect {| (Int64 1694217636694) |}]
;;

let%expect_test "write and read parquet series" =
  let len = 10 in
  let series =
    [| ( "timestamp"
       , Any_bigarray.Int64 (Array1.init Nativeint c_layout len Nativeint.of_int_exn) )
     ; "price", Float32 (Array1.init Float32 c_layout len Float.of_int)
     ; "qty", Float32 (Array1.init Float32 c_layout len Float.of_int)
     ; ( "is_bid"
       , Boolean
           (Array1.init Int8_unsigned c_layout len (fun v -> if v % 2 = 0 then 1 else 0))
       )
    |]
  in
  let test ~copy =
    let df = create ~copy series in
    let filepath = Filename_unix.temp_file "write_parquet_columns" ".parquet" in
    let (_ : int) = write_parquet df filepath in
    print_endline (Md5.digest_file_blocking filepath |> Md5.to_hex);
    [%expect {| 4ace1a010014b5fe1c27d8ef81945646 |}];
    let df' = read_parquet filepath in
    let series = series_bigarrays_exn df' in
    Array.iter series ~f:(fun (name, column) ->
      printf "%9s: " name;
      match column with
      | Any_bigarray.Boolean arr -> Print_bigarray.print_uint8 arr
      | Int64 arr -> Print_bigarray.print_nativeint arr
      | Float32 arr -> Print_bigarray.print_float arr
      | Float64 arr -> Print_bigarray.print_float arr);
    [%expect
      {|
    timestamp: [0 1 2 3 4 5 6 7 8 9]
        price: [0.0000 1.0000 2.0000 3.0000 4.0000 5.0000 6.0000 7.0000 8.0000 9.0000]
          qty: [0.0000 1.0000 2.0000 3.0000 4.0000 5.0000 6.0000 7.0000 8.0000 9.0000]
       is_bid: [1 0 1 0 1 0 1 0 1 0] |}]
  in
  test ~copy:true;
  test ~copy:false
;;

let%expect_test "series create" =
  let ba = Any_bigarray.Float32 (Array1.init Float32 c_layout 10 Float.of_int) in
  let test ~copy =
    let series = Series.create ~copy "test" ba in
    (match Series.to_bigarray_exn series with
     | Any_bigarray.Float32 arr -> Print_bigarray.print_float arr
     | _ -> failwith "wrong series dtype");
    [%expect
      {| [0.0000 1.0000 2.0000 3.0000 4.0000 5.0000 6.0000 7.0000 8.0000 9.0000] |}]
  in
  test ~copy:true;
  test ~copy:false
;;

let%expect_test "series is_empty" =
  let ba = Any_bigarray.Float32 (Array1.create Float32 c_layout 0) in
  let series = Series.create ~copy:false "test" ba in
  [%test_result: bool] ~expect:true (Series.is_empty series);
  let ba = Any_bigarray.Float32 (Array1.init Float32 c_layout 1 Float.of_int) in
  let series = Series.create ~copy:false "test" ba in
  [%test_result: bool] ~expect:false (Series.is_empty series)
;;

let%expect_test "select columns and rename" =
  let df = Polars_testdata.trades1 () in
  let df = select df [| "timestamp"; "price"; "qty" |] in
  rename_in_place df ~col:"qty" ~name:"volume";
  Polars.print df;
  [%expect
    {|
    shape: (223, 3)
    | timestamp     | price        | volume |
    | ---           | ---          | ---    |
    | i64           | f32          | f32    |
    |---------------|--------------|--------|
    | 1694217604155 | 25899.900391 | 0.005  |
    | 1694217604162 | 25899.800781 | 1.02   |
    | 1694217604185 | 25899.900391 | 0.019  |
    | 1694217604189 | 25899.800781 | 0.086  |
    | …             | …            | …      |
    | 1694217660053 | 25900.099609 | 0.001  |
    | 1694217660288 | 25900.0      | 0.007  |
    | 1694217660344 | 25900.099609 | 0.004  |
    | 1694217660528 | 25900.099609 | 0.016  | |}]
;;

let%expect_test "datetime to i64" =
  let df =
    Polars_testdata.trades1 ()
    |> Polars.sub ~pos:0 ~len:4
    |> Polars.cast ~col:"timestamp" ~dtype:(Datetime Milliseconds)
  in
  let dtype = Polars.select_by_name_exn df ~col:"timestamp" |> Series.dtype in
  print_s [%sexp (dtype : data_type)];
  [%expect {| (Datetime Milliseconds) |}];
  Polars_testing.print_col_i64 df ~col:"timestamp";
  [%expect {| [1694217604155 1694217604162 1694217604185 1694217604189] |}]
;;

let%expect_test "cast timestamp to float64 and rechunk" =
  let df = Polars_testdata.trades1 () in
  let () =
    Polars.select_by_name_exn df ~col:"timestamp"
    |> Polars.Series.cast ~dtype:Float64
    |> Polars.Series.rechunk
    |> Polars.Series.sub ~pos:0 ~len:4
    |> Polars.Series.to_bigarray_exn
    |> Polars_testing.print_f64_series
  in
  [%expect
    {| [1694217604155.0000 1694217604162.0000 1694217604185.0000 1694217604189.0000] |}]
;;

let%expect_test "dataframe is_empty" =
  let df = Polars_testdata.trades1 () |> Polars.sub ~pos:0 ~len:0 in
  [%test_result: int] ~expect:0 (Polars.length df);
  [%test_result: bool] ~expect:true (Polars.is_empty df)
;;

let%expect_test "get rows" =
  let df = Polars_testdata.trades1 () in
  let width = Polars.width df in
  for idx = 0 to Polars.length df - 1 do
    let row = Polars.get_row_exn df ~idx in
    [%test_result: int] ~expect:width (Array.length row)
  done;
  Printexc.record_backtrace false;
  ignore (Polars.get_row_exn df ~idx:666)
[@@expect.uncaught_exn
  {|
  (Failure
    "OutOfBounds(ErrString(\"index 666 is out of bounds for sequence of length 223\"))") |}]
;;
