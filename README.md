# ocaml-polars

API will change without notice.

TODO: split async api to `polars.async`

## see also

- [mt-caret/ocaml-polars](https://github.com/mt-caret/polars-ocaml) - project that was not ready when I needed it but it might be better for your use case

I started this project by writing function for parsing CSV and it grew from there without any ambition to be useful outside of my project.
It was separated into this library to avoid rebuilding entire polars library in the rest of my project which might save up to 4 minutes and 20 seconds of compilation time.

Released under MIT license.
