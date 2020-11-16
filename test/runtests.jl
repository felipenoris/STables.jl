
# Basic Table tests

import STables
using Test
using DataFrames
using Dates
import DelimitedFiles

# create table with test data
function create_table_a()
    col_names = [:C_STRING, :C_INT, :C_FLOAT, :C_NSTRING, :C_NINT, :C_NFLOAT]
    col_types = [String, Int, Float64, Union{Missing, String}, Union{Missing, Int}, Union{Missing, Float64}]
    ta_schema = STables.Schema(col_names, col_types)
    rows = 2
    ta = STables.Table(ta_schema, rows)
    ta[1,1] = "1,1"
    ta[1,2] = 5
    ta[1,3] = 2.2
    ta[1,4] = "1;4"
    ta[1,5] = 5
    ta[1,6] = 2.3

    ta[2,1] = "1;1"
    ta[2,2] = 5
    ta[2,3] = 2.2
    ta[2,4] = missing
    ta[2,5] = missing
    ta[2,6] = 2.3

    return ta
end

@testset "Grisu" begin
    @test STables.tostring(0.58) == "0.58"
    @test STables.tostring(15.2) == "15.2"
    @test STables.tostring(.000222) == "0.000222"
    @test STables.tostring(-.2) == "-0.2"
    @test STables.tostring(NaN) == "NaN"
    @test parse(Float64, STables.tostring(15.2)) ≈ 15.2
end

@testset "Schema" begin
    let
        s = STables.Schema(a=String, b=Union{Missing, String})
        @test s.names == [:a, :b]
        @test s.types == [String, Union{Missing, String}]
    end

    let
        s = STables.Schema(a=String, b=[1,2])
        @test s.names == [:a, :b]
        @test s.types == [String, Int]
    end

    let
        s = STables.Schema(a=[1,2], b=["a", missing])
        @test s.names == [:a, :b]
        @test s.types == [Int, Union{Missing, String}]
    end

    let
        s = STables.Schema(a=[1,2], b=["a", missing])
        @test s.names == [:a, :b]
        @test s.types == [Int, Union{String, Missing}]
    end

    let
        sa = STables.Schema([:a => String, :b => Int, :c => String])
        sb = STables.Schema([:a => String, :b => Int, :c => String])
        @test sa == sb
    end

    @testset "Copying" begin
        sch = STables.Schema(a=Int, b=String)
        sch_copy = copy(sch)
        @test isequal(sch, sch_copy)
        push!(sch, :c => Float64)
        @test !isequal(sch, sch_copy)
    end

    @testset "deepcopy has the same effects for schema" begin
        # deepcopy has the same effects for schema
        sch = STables.Schema(a=Int, b=String)
        sch_copy = deepcopy(sch)
        @test isequal(sch, sch_copy)
        push!(sch, :c => Float64)
        @test !isequal(sch, sch_copy)
    end

    let
        p = [:a => String, :b => Int]
        s = STables.Schema(p)
        @test p == STables.pairs(s)
    end
end

@testset "Table" begin
    @testset "Constructor" begin
        tb = STables.Table(a=[1, 2], b=[3, missing])
        @test tb[:a] == [ 1 , 2 ]
        @test isequal(tb[:b], [3, missing])
    end

    @testset "Create Column" begin
        @testset "missing" begin
            y = STables._create_table_column(Union{Missing, Int}, 2)
            @test typeof(y) == Vector{Union{Missing, Int}}
            @test length(y) == 2
            @test ismissing(y[2])
        end

        @testset "string" begin
            z = STables._create_table_column(String, 2)
            @test typeof(z) == Vector{String}
            @test length(z) == 2
            @test z[2] == ""
        end
    end

    @testset "Create Table" begin
        ta = create_table_a()

        @test size(ta) == (2,6)
        @test typeof(ta[:C_STRING]) == Vector{String}
        @test typeof(ta[:C_INT]) == Vector{Int}
        @test typeof(ta[:C_FLOAT]) == Vector{Float64}
        @test typeof(ta[:C_NSTRING]) == Vector{Union{Missing, String}}
        @test typeof(ta[:C_NINT]) == Vector{Union{Missing, Int}}
        @test typeof(ta[:C_NFLOAT]) == Vector{Union{Missing, Float64}}

        @test ta[1,1] == "1,1"
        @test ta[1,2] == 5
        @test ta[1,3] == 2.2
        @test ta[1,4] == "1;4"
        @test ta[1,5] == 5
        @test ta[1,6] == 2.3

        @test ta[2,1] == "1;1"
        @test ta[2,2] == 5
        @test ta[2,3] == 2.2
        @test ismissing(ta[2,4])
        @test ismissing(ta[2,5])
        @test ta[2,6] == 2.3

        STables.TableRow(ta, 1)[:] == [ "1,1", 5, 2.2, "1;4", 5, 2.3 ]
        isequal(STables.TableRow(ta, 2)[:], [ "1;1", 5, 2.2, missing, missing, 2.3 ])

        # Scalar attribution to column
        @test ta[:C_STRING] == ["1,1", "1;1"]
        ta[:C_STRING] = "string"
        @test ta[:C_STRING] == [ "string", "string"]

        # Restore original value using attribution to vector
        ta[:C_STRING] = ["1,1", "1;1"]
        @test ta[:C_STRING] == ["1,1", "1;1"]

        # Scalar attribution to nullable column
        tmp = ["1;4", missing]
        ta[:C_NSTRING] = "10"
        @test ta[1, :C_NSTRING] == "10"
        @test ta[2, :C_NSTRING] == "10"
        ta[:C_NSTRING] = missing
        @test ismissing(ta[1, :C_NSTRING])
        @test ismissing(ta[2, :C_NSTRING])


        ta[:C_NSTRING] = tmp
        @test ta[1, :C_NSTRING] == "1;4"
        @test ismissing(ta[2, :C_NSTRING])

        # Test again all table values
        @test ta[1,1] == "1,1"
        @test ta[1,2] == 5
        @test ta[1,3] == 2.2
        @test ta[1,4] == "1;4"
        @test ta[1,5] == 5
        @test ta[1,6] == 2.3

        @test ta[2,1] == "1;1"
        @test ta[2,2] == 5
        @test ta[2,3] == 2.2
        @test ismissing(ta[2,4])
        @test ismissing(ta[2,5])
        @test ta[2,6] == 2.3
    end

    @testset "eachrow" begin
        # eachrow
        sch = STables.Schema([:a => String, :b => Int, :c => String])
        tb = STables.Table(sch, 5)
        tb[:a] = "fixed-"

        i = 1
        for r in STables.eachrow(tb)
            r[:c] = string(i)
            i += 1
        end

        @test_throws AssertionError tb[:d]

        tb[:d] = tb[:a] .* tb[:c]

        @test tb[:d] == [ "fixed-1", "fixed-2", "fixed-3", "fixed-4", "fixed-5"]
        @test tb[:a] == fill("fixed-", 5)
        @test tb[:c] == [ "1", "2", "3", "4", "5"]
        @test tb[:b] == fill(0, 5)
    end

    @testset "Thousands Separator" begin

        # Table tests with thousands_separator
        fm = STables.CSVFormat(thousands_separator='.', date_format=Dates.DateFormat("dd/mm/Y"))
        tb_example_csv = STables.readcsv("example.csv", [String, Int, Float64, Date], fm)

        #=
        str1;10;10.000,23
        str2;-20;200,23
        str3;0;20.200.100,00
        str4;1.000;1000
        str5;1.000.000;1.000
        str6;1;1000,00
        =#

        @test tb_example_csv[1,1] == "str1"
        @test tb_example_csv[1,2] == 10
        @test tb_example_csv[1,3] == 1000
        @test tb_example_csv[1,4] == Date(2016,1,2)

        @test tb_example_csv[2,1] == "str2"
        @test tb_example_csv[2,2] == -20
        @test tb_example_csv[2,3] == 200.23
        @test tb_example_csv[2,4] == Date(2016,1,3)

        @test tb_example_csv[3,1] == "str3"
        @test tb_example_csv[3,2] == 0
        @test tb_example_csv[3,3] == 20200100.0
        @test tb_example_csv[3,4] == Date(2016,1,4)

        @test tb_example_csv[4,1] == "str4"
        @test tb_example_csv[4,2] == 1000
        @test tb_example_csv[4,3] == 10000.23
        @test tb_example_csv[4,4] == Date(2016,1,25)

        @test tb_example_csv[5,1] == "str5"
        @test tb_example_csv[5,2] == 1000000
        @test tb_example_csv[5,3] == 1000.0
        @test tb_example_csv[5,4] == Date(2016,1,26)

        @test tb_example_csv[6,1] == "str6"
        @test tb_example_csv[6,2] == 1
        @test tb_example_csv[6,3] == -1000.0
        @test tb_example_csv[6,4] == Date(2016,1,27)

        @testset "collect a TableRow" begin
            @test collect(STables.TableRow(tb_example_csv, 1)) == ["str1", 10, 1000, Date(2016,1,2)]
        end
    end

    @testset "append a row" begin
        tb = STables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ 10.0, 20.0, missing] )
        row = [ 4, "four", missing]
        append!(tb, row)
        @test tb[:a] == [1, 2, 3, 4]
        @test tb[:b] == ["one", "two", "three", "four"]
        @test isequal(tb[:c], [ 10.0, 20.0, missing, missing])

        tr = STables.TableRow(tb, 2)
        @test tr[1] == 2
        @test tr[:a] == 2
        @test tr[2] == "two"
        @test tr[:b] == "two"
        @test tr[3] == 20.0
        @test tr[:c] == 20.0
    end

    @testset "append a matrix" begin
        # append a matrix
        tb = STables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ 10.0, 20.0, missing] )

        mat = [ 4 "four" 40.0;
                5 "five" missing
              ]

        append!(tb, mat)
        @test tb[:a] == [1, 2, 3, 4, 5]
        @test tb[:b] == ["one", "two", "three", "four", "five"]
        @test isequal(tb[:c], [ 10.0, 20.0, missing, 40.0, missing])
    end

    @testset "append a table" begin
        # append a table
        tb = STables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ 10.0, 20.0, missing] )
        tb2 = STables.Table(a=[4, 5], b=["four", "five"], c=[ 40.0, missing ])
        append!(tb, tb2)
        @test tb[:a] == [1, 2, 3, 4, 5]
        @test tb[:b] == ["one", "two", "three", "four", "five"]
        @test isequal(tb[:c], [ 10.0, 20.0, missing, 40.0, missing ])
    end

    @testset "Copying Tables" begin
        tb = STables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ 10.0, 20.0, missing] )
        tb_copy = copy(tb)
        @test isequal(tb, tb_copy)
        append!(tb, [ 4, "four", 40.0 ])
        @test isequal(tb, tb_copy) # shallow-copy will preserve equality on adding rows
        tb[:d] = [1, 2, 3, 4]
        @test !isequal(tb, tb_copy) # shallow-copy will not preserve equality on adding columns
    end

    @testset "Deepcopy for Tables" begin
        tb = STables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ 10.0, 20.0, missing ] )
        tb_copy = deepcopy(tb)
        @test isequal(tb, tb_copy)
        append!(tb, [ 4, "four", 40.0])
        @test !isequal(tb, tb_copy) # deepcopy will not preserve equality on adding rows
    end

    @testset "vcat row" begin
        tb = STables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ 10.0, 20.0, missing ] )
        row = [ 4, "four", 40.0 ]
        tb_new = [ tb ; row]
        @test tb_new[:a] == [1, 2, 3, 4]
        @test tb_new[:b] == ["one", "two", "three", "four"]
        @test size(tb) == (3, 3) # shouldn't have side-effects on original table
    end

    @testset "vcat matrix" begin
        tb = STables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ 10.0, 20.0, missing ] )

        mat = [ 4 "four" 40.0;
                5 "five" missing
              ]

        tb_new = [ tb ; mat ]
        @test tb_new[:a] == [1, 2, 3, 4, 5]
        @test tb_new[:b] == ["one", "two", "three", "four", "five"]
        @test size(tb) == (3, 3) # shouldn't have side-effects on original table
    end

    @testset "vcat table" begin
        tb = STables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ 10.0, 20.0, missing ] )
        tb2 = STables.Table(a=[4, 5], b=["four", "five"], c=[ 40.0, missing ])
        tb_new = [tb; tb2]
        @test tb_new[:a] == [1, 2, 3, 4, 5]
        @test tb_new[:b] == ["one", "two", "three", "four", "five"]
        @test size(tb) == (3, 3) # shouldn't have side-effects on original table
        @test size(tb2) == (2, 3) # shouldn't have side-effects on original table
    end
end

@testset "CSV" begin
    FP_TA_CSV = joinpath(dirname(@__FILE__), "ta.csv")
    ta = create_table_a()

    try
        STables.writecsv(FP_TA_CSV, ta)
        lines = readlines(FP_TA_CSV)
        @test length(lines) == 3
        @test lines[1] == "C_STRING;C_INT;C_FLOAT;C_NSTRING;C_NINT;C_NFLOAT"
        @test lines[2] == "1,1;5;2,2;\"1;4\";5;2,3"
        @test lines[3] == "\"1;1\";5;2,2;;;2,3"

        col_names = [:C_STRING, :C_INT, :C_FLOAT, :C_NSTRING, :C_NINT, :C_NFLOAT]
        col_types = [String, Int, Float64, Union{Missing, String}, Union{Missing, Int}, Union{Missing, Float64}]
        ta_schema = STables.Schema(col_names, col_types)

        tb = STables.readcsv(FP_TA_CSV, col_types)
        @test names(tb) == col_names
        @test tb[1,1] == "1,1"
        @test tb[1,2] == 5
        @test tb[1,3] == 2.2
        @test tb[1,4] == "1;4"
        @test tb[1,5] == 5
        @test tb[1,6] == 2.3
        @test tb[2,1] == "1;1"
        @test tb[2,2] == 5
        @test tb[2,3] == 2.2
        @test ismissing(tb[2,4])
        @test ismissing(tb[2,5])
        @test tb[2,6] == 2.3

        fm = STables.CSVFormat(decimal_separator='.')
        STables.writecsv(FP_TA_CSV, ta, fm; header=false)
        lines = readlines(FP_TA_CSV)
        @test length(lines) == 2
        @test chomp(lines[1]) == "1,1;5;2.2;\"1;4\";5;2.3"
        @test chomp(lines[2]) == "\"1;1\";5;2.2;;;2.3"

        fm = STables.CSVFormat(decimal_separator='.')
        tb = STables.readcsv(FP_TA_CSV, ta_schema, fm; header=false)
        @test names(tb) == col_names
        @test tb[1,1] == "1,1"
        @test tb[1,2] == 5
        @test tb[1,3] == 2.2
        @test tb[1,4] == "1;4"
        @test tb[1,5] == 5
        @test tb[1,6] == 2.3
        @test tb[2,1] == "1;1"
        @test tb[2,2] == 5
        @test tb[2,3] == 2.2
        @test ismissing(tb[2,4])
        @test ismissing(tb[2,5])
        @test tb[2,6] == 2.3
    finally
        isfile(FP_TA_CSV) && rm(FP_TA_CSV)
    end
end

@testset "Schema Inference" begin

    @testset "valid integers without thousands separator" begin
        fm = STables.CSVFormat(decimal_separator='.')
        ir = STables.integer_regex(fm)
        @test occursin(ir, "0")
        @test occursin(ir, "1203")
        @test occursin(ir, "-1")
        @test !occursin(ir, "am")
        @test !occursin(ir, "-1,23")
        @test !occursin(ir, "-1.23")
        @test !occursin(ir, "123.123")
        @test !occursin(ir, "23.10.2015")
        @test !occursin(ir, "23.10.15")
        @test !occursin(ir, "1,2,3")
    end

    @testset "valid integers with thousands separator" begin
        let
            fm = STables.CSVFormat(decimal_separator=',', thousands_separator='.')
            ir = STables.integer_regex(fm)
            @test occursin(ir, "0")
            @test occursin(ir, "-1")
            @test !occursin(ir, "am")
            @test !occursin(ir, "-1,23")
            @test !occursin(ir, "-1.23")
            @test occursin(ir, "123.123")
            @test !occursin(ir, "23.10.2015")
            @test !occursin(ir, "23.10.15")
            @test !occursin(ir, "1,2,3")
            @test !occursin(ir, "1.2.3")
            @test occursin(ir, "10")
            @test occursin(ir, "100")
            @test !occursin(ir, "1203")
            @test occursin(ir, "1.203")
            @test occursin(ir, "10.222")
            @test occursin(ir, "100.222")
            @test occursin(ir, "1.000.222")
            @test occursin(ir, "10.000.222")
            @test occursin(ir, "100.000.222")
            @test occursin(ir, "1.000.000.222")
            @test occursin(ir, "01.000.000.222")
        end

        let
            fm = STables.CSVFormat(decimal_separator='.', thousands_separator=',')
            ir = STables.integer_regex(fm)
            @test occursin(ir, "0")
            @test occursin(ir, "-1")
            @test !occursin(ir, "am")
            @test !occursin(ir, "-1.23")
            @test !occursin(ir, "-1.23")
            @test occursin(ir, "123,123")
            @test !occursin(ir, "23,10,2015")
            @test !occursin(ir, "23.10.15")
            @test !occursin(ir, "1,2,3")
            @test !occursin(ir, "1.2.3")
            @test occursin(ir, "10")
            @test occursin(ir, "100")
            @test !occursin(ir, "1203")
            @test occursin(ir, "1,203")
            @test occursin(ir, "10,222")
            @test occursin(ir, "100,222")
            @test occursin(ir, "1,000,222")
            @test occursin(ir, "10,000,222")
            @test occursin(ir, "100,000,222")
            @test occursin(ir, "1,000,000,222")
            @test !occursin(ir, "1.000.000.222")
            @test occursin(ir, "01,000,000,222")
        end
    end

    @testset "valid floats without thousands separator" begin
        fm = STables.CSVFormat(decimal_separator='.')
        ir = STables.float_regex(fm)
        @test occursin(ir, "0")
        @test occursin(ir, "1203")
        @test occursin(ir, "-1")
        @test !occursin(ir, "am")
        @test !occursin(ir, "-1,23")
        @test occursin(ir, "-1.23")
        @test occursin(ir, "123.123")
        @test !occursin(ir, "23.10.2015")
        @test !occursin(ir, "23.10.15")
        @test !occursin(ir, "1,2,3")
        @test !occursin(ir, "1.2.3")
        @test occursin(ir, "123123123.123123123123")
        @test occursin(ir, "123123123.")
        @test occursin(ir, ".0000")
        @test !occursin(ir, ".")
        @test occursin(ir, ".0")
        @test occursin(ir, "-.0")
    end

    @testset "valid floats with thousands separator" begin
        fm = STables.CSVFormat(thousands_separator='.')
        ir = STables.float_regex(fm)
        @test occursin(ir, "0")
        @test occursin(ir, "-1")
        @test !occursin(ir, "am")
        @test occursin(ir, "-1,23")
        @test !occursin(ir, "-1.23")
        @test occursin(ir, "123.123")
        @test !occursin(ir, "23.10.2015")
        @test !occursin(ir, "23.10.15")
        @test !occursin(ir, "1,2,3")
        @test !occursin(ir, "1.2.3")
        @test occursin(ir, "10")
        @test occursin(ir, "100")
        @test !occursin(ir, "1203")
        @test occursin(ir, "1.203")
        @test occursin(ir, "10.222")
        @test occursin(ir, "100.222")
        @test occursin(ir, "1.000.222")
        @test occursin(ir, "10.000.222")
        @test occursin(ir, "100.000.222")
        @test occursin(ir, "1.000.000.222")
        @test occursin(ir, "01.000.000.222")
        @test occursin(ir, "10,1")
        @test occursin(ir, "100,1")
        @test !occursin(ir, "1203,1123123123")
        @test occursin(ir, "1.203,1")
        @test occursin(ir, "10.222,1")
        @test occursin(ir, "100.222,1")
        @test occursin(ir, "1.000.222,1")
        @test occursin(ir, "10.000.222,1")
        @test occursin(ir, "-100.000.222,1")
        @test occursin(ir, "1.000.000.222,1")
        @test occursin(ir, "01.000.000.222,1")
    end

    @testset "valid floats with thousands separator" begin
        fm = STables.CSVFormat(decimal_separator='.', thousands_separator=',')
        ir = STables.float_regex(fm)
        @test occursin(ir, "0")
        @test occursin(ir, "-1")
        @test !occursin(ir, "am")
        @test !occursin(ir, "-1,23")
        @test occursin(ir, "-1.23")
        @test occursin(ir, "123,123")
        @test !occursin(ir, "23.10.2015")
        @test !occursin(ir, "23.10.15")
        @test !occursin(ir, "1,2,3")
        @test !occursin(ir, "1.2.3")
        @test occursin(ir, "10")
        @test occursin(ir, "100")
        @test !occursin(ir, "1203")
        @test occursin(ir, "1,203")
        @test occursin(ir, "10,222")
        @test occursin(ir, "100,222")
        @test occursin(ir, "1,000,222")
        @test occursin(ir, "10,000,222")
        @test occursin(ir, "100,000,222")
        @test occursin(ir, "1,000,000,222")
        @test occursin(ir, "01,000,000,222")
        @test occursin(ir, "10.1")
        @test occursin(ir, "100.1")
        @test !occursin(ir, "1203.1123123123")
        @test occursin(ir, "1,203.1")
        @test occursin(ir, "10,222.1")
        @test occursin(ir, "100,222.1")
        @test occursin(ir, "1,000,222.1")
        @test occursin(ir, "10,000,222.1")
        @test occursin(ir, "-100,000,222.1")
        @test occursin(ir, "1,000,000,222.1")
        @test occursin(ir, "01,000,000,222.1")
    end

    @testset "read csv with type inference" begin
        fm = STables.CSVFormat(thousands_separator='.', date_format=Dates.DateFormat("dd/mm/Y"))
        raw = DelimitedFiles.readdlm("example.csv", fm.dlm, String)

        let
            s = STables.infer_type(raw[2,3], fm)
            @test s == STables.InferenceState(Int, false)
            STables.infer_type(raw[3,3], fm, s)
            @test s == STables.InferenceState(Float64,false)
            STables.infer_type(raw[5,3], fm, s)
            @test s == STables.InferenceState(Float64,false) # should not go back to Int
        end

        sch = STables.infer_schema(raw, fm)
        @test sch == STables.Schema([:COL_A,:COL_B,:COL_C,:COL_D], [String,Int64,Float64,Date])

        tb = STables.readcsv("example.csv", sch, fm)

        fm2 = STables.CSVFormat(decimal_separator='.', date_format=Dates.DateFormat("dd/mm/Y"))
        tb_copy = STables.readcsv("example_no_ts.csv", fm2)
        @test isequal(tb, tb_copy)

        fm = STables.CSVFormat(dlm=',', decimal_separator='.')
        tb = STables.readcsv("example_nullable.csv", fm; header=false)
        @test tb[1,1] == 1
        @test ismissing(tb[2,1])
        @test tb[3,1] == 3
        @test tb[1,2] == "Ab Cd"
        @test ismissing(tb[2,2])
        @test ismissing(tb[3,2])
        @test tb[1,3] == 100.5
        @test ismissing(tb[2,3])
        @test tb[3,3] == 2.0
    end
end

@testset "DataFrames" begin
    @testset "name" begin
        sch = STables.Schema([:a, :b, :c, :d], [Int, String, Bool, Union{Missing, Int}])
        tb = STables.Table(sch, 3)
        tb[:a] = [1, 2, 3]
        tb[:b] = ["1", "2", "3"]
        tb[:c] = [false, true, false]
        tb[:d] = [1, 2, missing]

        rename!(tb, [:a, :b, :c, :d])
        @test tb[1, 1] == 1
        @test tb[1, 2] == "1"
        @test tb[1, 3] == false
        @test ismissing(tb[3, 4])
        @test tb[1, 4] == 1

        @test names(tb) == [ :a, :b, :c, :d ]
    end

    @testset "Table with dataframe" begin
        # Table with DataFrame
        df = DataFrame(a = [1, missing], b = Union{Missing, Symbol}[:a, :b])
        df_types = [Union{Missing, Int}, Union{Missing, Symbol}]
        df_schema = STables.Schema([:col1, :col2], df_types)
        df_table = STables.Table(df_schema, df)

        @test ismissing(df_table[2,1])
        @test df_table[1,1] == 1
        @test df_table[1,2] == :a
        @test df_table[2,2] == :b

        df_table2 = STables.Table([Union{Missing, Int}, Union{Missing, Symbol}], df)
        @test ismissing(df_table2[2,1])
        @test df_table2[1,1] == 1
        @test df_table2[1,2] == :a
        @test df_table2[2,2] == :b

        df_table3 = STables.Table(df)
        @test ismissing(df_table3[2,1])
        @test df_table3[1,1] == 1
        @test df_table3[1,2] == :a
        @test df_table3[2,2] == :b
    end
end
