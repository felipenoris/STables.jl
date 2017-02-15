
# Basic Table tests

using Tables
using Base.Test
using NullableArrays
using DataFrames
using Lifting

# Grisu
@test Tables.tostring(0.58) == "0.58"
@test Tables.tostring(15.2) == "15.2"
@test Tables.tostring(.000222) == "0.000222"
@test Tables.tostring(-.2) == "-0.2"
@test Tables.tostring(NaN) == "NaN"
@test isapprox(parse(Tables.tostring(15.2)), 15.2)

s = Tables.Schema(a=String, b=Nullable{String})
@test s.names == [:a, :b]
@test s.types == [String, Nullable{String}]

ss = Tables.Schema(a=String, b=[1,2])
@test ss.names == [:a, :b]
@test ss.types == [String, Int]

sss = Tables.Schema(a=[1,2], b=NullableArray(["a", "b"]))
@test sss.names == [:a, :b]
@test sss.types == [Int, Nullable{String}]

tb = Table(a=[1,2], b=NullableArray([3,Nullable{Int}()]))
@test tb[:a] == [ 1 , 2 ]
@test get(tb[:b][1] == Nullable(3))
@test isnull(tb[:b][2])

y = Tables._create_table_column(Nullable{Int}, 2)
@test typeof(y) == NullableArrays.NullableArray{Int,1}
@test length(y) == 2
@test isnull(y[2])

z = Tables._create_table_column(String, 2)
@test typeof(z) == Array{String, 1}
@test length(z) == 2

zz = Tables._create_table_column(Nullable{Int}, 5)

col_names = [:C_STRING, :C_INT, :C_FLOAT, :C_NSTRING, :C_NINT, :C_NFLOAT]
col_types = [String, Int, Float64, Nullable{String}, Nullable{Int}, Nullable{Float64}]
ta_schema = Tables.Schema(col_names, col_types)
rows = 2
ta = Tables.Table(ta_schema, rows)
@test size(ta) == (2,6)
@test typeof(ta[:C_STRING]) == Vector{String}
@test typeof(ta[:C_INT]) == Vector{Int}
@test typeof(ta[:C_FLOAT]) == Vector{Float64}
@test typeof(ta[:C_NSTRING]) == NullableVector{String}
@test typeof(ta[:C_NINT]) == NullableVector{Int}
@test typeof(ta[:C_NFLOAT]) == NullableVector{Float64}
ta[1,1] = "1,1"
ta[1,2] = 5
ta[1,3] = 2.2
ta[1,4] = "1;4"
ta[1,5] = 5
ta[1,6] = 2.3

ta[2,1] = "1;1"
ta[2,2] = 5
ta[2,3] = 2.2
ta[2,4] = Nullable{String}()
ta[2,5] = Nullable{Int}()
ta[2,6] = 2.3

@test ta[1,1] == "1,1"
@test ta[1,2] == 5
@test ta[1,3] == 2.2
@test lift(ta[1,4] == unlift("1;4"))
@test lift(ta[1,5] == unlift(5))
@test lift(ta[1,6] == unlift(2.3))

@test ta[2,1] == "1;1"
@test ta[2,2] == 5
@test ta[2,3] == 2.2
@test isnull(ta[2,4])
@test isnull(ta[2,5])
@test lift(ta[2,6] == unlift(2.3))

# Scalar attribution to column
@test ta[:C_STRING] == ["1,1", "1;1"]
ta[:C_STRING] = "string"
@test ta[:C_STRING] == [ "string", "string"]

# Restore original value using attribution to vector
ta[:C_STRING] = ["1,1", "1;1"]
@test ta[:C_STRING] == ["1,1", "1;1"]

# Scalar attribution to nullable column
tmp = NullableArray{String}(["1;4", Nullable{String}()])
ta[:C_NSTRING] = "10"
@test get(ta[1, :C_NSTRING] == Nullable("10"))
@test get(ta[2, :C_NSTRING] == Nullable("10"))
ta[:C_NSTRING] = Nullable{String}()
@test isnull(ta[1, :C_NSTRING])
@test isnull(ta[2, :C_NSTRING])
ta[:C_NSTRING] = tmp
@test get(ta[1, :C_NSTRING] == Nullable("1;4"))
@test isnull(ta[2, :C_NSTRING])

# Test again all table values
@test ta[1,1] == "1,1"
@test ta[1,2] == 5
@test ta[1,3] == 2.2
@test lift(ta[1,4] == unlift("1;4"))
@test lift(ta[1,5] == unlift(5))
@test lift(ta[1,6] == unlift(2.3))

@test ta[2,1] == "1;1"
@test ta[2,2] == 5
@test ta[2,3] == 2.2
@test isnull(ta[2,4])
@test isnull(ta[2,5])
@test lift(ta[2,6] == unlift(2.3))

FP_TA_CSV = joinpath(dirname(@__FILE__), "ta.csv")

try
    Tables.writecsv(FP_TA_CSV, ta)
    lines = readlines(FP_TA_CSV)
    @test length(lines) == 3
    @test chomp(lines[1]) == "C_STRING;C_INT;C_FLOAT;C_NSTRING;C_NINT;C_NFLOAT"
    @test chomp(lines[2]) == "1,1;5;2,2;\"1;4\";5;2,3"
    @test chomp(lines[3]) == "\"1;1\";5;2,2;;;2,3"

    tb = Tables.readcsv(FP_TA_CSV, col_types)
    @test names(tb) == col_names
    @test tb[1,1] == "1,1"
    @test tb[1,2] == 5
    @test tb[1,3] == 2.2
    @test lift(tb[1,4] == unlift("1;4"))
    @test lift(tb[1,5] == unlift(5))
    @test lift(tb[1,6] == unlift(2.3))
    @test tb[2,1] == "1;1"
    @test tb[2,2] == 5
    @test tb[2,3] == 2.2
    @test isnull(tb[2,4])
    @test isnull(tb[2,5])
    @test lift(tb[2,6] == unlift(2.3))

    fm = Tables.CSVFormat(decimal_separator='.')
    Tables.writecsv(FP_TA_CSV, ta, fm; header=false)
    lines = readlines(FP_TA_CSV)
    @test length(lines) == 2
    @test chomp(lines[1]) == "1,1;5;2.2;\"1;4\";5;2.3"
    @test chomp(lines[2]) == "\"1;1\";5;2.2;;;2.3"

    fm = Tables.CSVFormat(decimal_separator='.')
    tb = Tables.readcsv(FP_TA_CSV, ta_schema, fm; header=false)
    @test names(tb) == col_names
    @test tb[1,1] == "1,1"
    @test tb[1,2] == 5
    @test tb[1,3] == 2.2
    @test lift(tb[1,4] == unlift("1;4"))
    @test lift(tb[1,5] == unlift(5))
    @test lift(tb[1,6] == unlift(2.3))
    @test tb[2,1] == "1;1"
    @test tb[2,2] == 5
    @test tb[2,3] == 2.2
    @test isnull(tb[2,4])
    @test isnull(tb[2,5])
    @test lift(tb[2,6] == unlift(2.3))
finally
    rm(FP_TA_CSV)
end

# Table tests with thousands_separator
fm = Tables.CSVFormat(thousands_separator=Nullable('.'), date_format=Dates.DateFormat("dd/mm/Y"))
tb_example_csv = Tables.readcsv("example.csv", [String, Int, Float64, Date], fm)

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

# eachrow
sch = Tables.Schema([:a, :b, :c, :d], [Int, String, Bool, Nullable{Int}])
tb = Tables.Table(sch, 3)
tb[:a] = [1, 2, 3]
tb[:b] = ["1", "2", "3"]
tb[:c] = [false, true, false]
tb[:d] = NullableArray([1, 2, Nullable{Int}()])

names!(tb, [:a, :b, :c, :d])
@test tb[1, 1] == 1
@test tb[1, 2] == "1"
@test tb[1, 3] == false
@test isnull(tb[3, 4])
@test get(tb[1, 4]) == 1

# Table with DataFrame
df = DataFrame(a = @data([1, NA]), b = [:a, :b])
df_types = [ Nullable{Int}, Nullable{Symbol} ]
df_schema = Tables.Schema([:col1, :col2], df_types)
df_table = Tables.Table(df_schema, df)
@test isnull(df_table[2,1]) == true
@test get(df_table[1,1]) == 1
@test get(df_table[1,2]) == :a
@test get(df_table[2,2]) == :b

df_table2 = Tables.Table([Nullable{Int}, Nullable{Symbol}], df)
@test isnull(df_table2[2,1]) == true
@test get(df_table2[1,1]) == 1
@test get(df_table2[1,2]) == :a
@test get(df_table2[2,2]) == :b

df_table3 = Tables.Table(df)
@test isnull(df_table3[2,1]) == true
@test get(df_table3[1,1]) == 1
@test get(df_table3[1,2]) == :a
@test get(df_table3[2,2]) == :b

sch = Schema( [:a => String, :b => Int, :c => String] )
tb = Tables.Table(sch, 5)
tb[:a] = "fixed-"

i = 1
for r in Tables.eachrow(tb)
    r[:c] = string(i)
    i += 1
end

tb[:d] = tb[:a] .* tb[:c]

@test tb[:d] == [ "fixed-1", "fixed-2", "fixed-3", "fixed-4", "fixed-5"]
@test tb[:a] == fill("fixed-", 5)
@test tb[:c] == [ "1", "2", "3", "4", "5"]
@test tb[:b] == fill(0, 5)

sa = Schema( [:a => String, :b => Int, :c => String] )
sb = Schema( [:a => String, :b => Int, :c => String] )
@test sa == sb

# append a row
tb = Tables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ Nullable(10.0), Nullable(20.0), Nullable{Float64}()] )
row = [ 4, "four", Nullable(40.0)]
append!(tb, row)
@test tb[:a] == [1, 2, 3, 4]
@test tb[:b] == ["one", "two", "three", "four"]
#@test get(tb[:c] == [ Nullable(10.0), Nullable(20.0), Nullable{Float64}(), Nullable(40.0)])

tr = Tables.TableRow(tb, 2)
@test tr[1] == 2
@test tr[:a] == 2
@test tr[2] == "two"
@test tr[:b] == "two"
@test get(tr[3] == Nullable(20.0))
@test get(tr[:c] == Nullable(20.0))

# append a matrix
tb = Tables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ Nullable(10.0), Nullable(20.0), Nullable{Float64}()] )
mat = Array{Any}(2,3)
mat[1,1] = 4
mat[2,1] = 5
mat[1,2] = "four"
mat[2,2] = "five"
mat[1,3] = Nullable(40.0)
mat[2,3] = Nullable{Float64}()
append!(tb, mat)
@test tb[:a] == [1, 2, 3, 4, 5]
@test tb[:b] == ["one", "two", "three", "four", "five"]
#@test get(tb[:c] == [ Nullable(10.0), Nullable(20.0), Nullable{Float64}(), Nullable(40.0), Nullable{Float64}()])

# append a table
tb = Tables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ Nullable(10.0), Nullable(20.0), Nullable{Float64}()] )
tb2 = Tables.Table(a=[4, 5], b=["four", "five"], c=[ Nullable(40.0), Nullable{Float64}() ])
append!(tb, tb2)
@test tb[:a] == [1, 2, 3, 4, 5]
@test tb[:b] == ["one", "two", "three", "four", "five"]
#@test get(tb[:c] == [ Nullable(10.0), Nullable(20.0), Nullable{Float64}(), Nullable(40.0), Nullable{Float64}()])

# Copying
sch = Schema(a=Int, b=String)
sch_copy = copy(sch)
@test isequal(sch, sch_copy)
push!(sch, :c => Float64)
@test !isequal(sch, sch_copy)

# deepcopy has the same effects for schema
sch = Schema(a=Int, b=String)
sch_copy = deepcopy(sch)
@test isequal(sch, sch_copy)
push!(sch, :c => Float64)
@test !isequal(sch, sch_copy)

# Copying Tables
tb = Tables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ Nullable(10.0), Nullable(20.0), Nullable{Float64}()] )
tb_copy = copy(tb)
@test isequal(tb, tb_copy)
append!(tb, [ 4, "four", Nullable(40.0)])
@test isequal(tb, tb_copy) # shallow-copy will preserve equality on adding rows
tb[:d] = [1, 2, 3, 4]
@test !isequal(tb, tb_copy) # shallow-copy will not preserve equality on adding columns

# Deepcopy for Tables
tb = Tables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ Nullable(10.0), Nullable(20.0), Nullable{Float64}()] )
tb_copy = deepcopy(tb)
@test isequal(tb, tb_copy)
append!(tb, [ 4, "four", Nullable(40.0)])
@test !isequal(tb, tb_copy) # deepcopy will not preserve equality on adding rows

# vcat row
tb = Tables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ Nullable(10.0), Nullable(20.0), Nullable{Float64}()] )
row = [ 4, "four", Nullable(40.0)]
tb_new = [ tb ; row]
@test tb_new[:a] == [1, 2, 3, 4]
@test tb_new[:b] == ["one", "two", "three", "four"]
@test size(tb) == (3, 3) # shouldn't have side-effects on original table

# vcat matrix
tb = Tables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ Nullable(10.0), Nullable(20.0), Nullable{Float64}()] )
mat = Array{Any}(2,3)
mat[1,1] = 4
mat[2,1] = 5
mat[1,2] = "four"
mat[2,2] = "five"
mat[1,3] = Nullable(40.0)
mat[2,3] = Nullable{Float64}()
tb_new = [ tb ; mat ]
@test tb_new[:a] == [1, 2, 3, 4, 5]
@test tb_new[:b] == ["one", "two", "three", "four", "five"]
@test size(tb) == (3, 3) # shouldn't have side-effects on original table

# vcat table
tb = Tables.Table(a=[1,2,3], b=["one", "two", "three"], c=[ Nullable(10.0), Nullable(20.0), Nullable{Float64}()] )
tb2 = Tables.Table(a=[4, 5], b=["four", "five"], c=[ Nullable(40.0), Nullable{Float64}() ])
tb_new = [tb; tb2]
@test tb_new[:a] == [1, 2, 3, 4, 5]
@test tb_new[:b] == ["one", "two", "three", "four", "five"]
@test size(tb) == (3, 3) # shouldn't have side-effects on original table
@test size(tb2) == (2, 3) # shouldn't have side-effects on original table

# Schema inference

# valid integers without thousands separator
fm = Tables.CSVFormat(decimal_separator='.')
ir = Tables.integer_regex(fm)
@test ismatch(ir, "0")
@test ismatch(ir, "1203")
@test ismatch(ir, "-1")
@test !ismatch(ir, "am")
@test !ismatch(ir, "-1,23")
@test !ismatch(ir, "-1.23")
@test !ismatch(ir, "123.123")
@test !ismatch(ir, "23.10.2015")
@test !ismatch(ir, "23.10.15")
@test !ismatch(ir, "1,2,3")

# valid integers with thousands separator
fm = Tables.CSVFormat(decimal_separator=',', thousands_separator=Nullable('.'))
ir = Tables.integer_regex(fm)
@test ismatch(ir, "0")
@test ismatch(ir, "-1")
@test !ismatch(ir, "am")
@test !ismatch(ir, "-1,23")
@test !ismatch(ir, "-1.23")
@test ismatch(ir, "123.123")
@test !ismatch(ir, "23.10.2015")
@test !ismatch(ir, "23.10.15")
@test !ismatch(ir, "1,2,3")
@test !ismatch(ir, "1.2.3")
@test ismatch(ir, "10")
@test ismatch(ir, "100")
@test !ismatch(ir, "1203")
@test ismatch(ir, "1.203")
@test ismatch(ir, "10.222")
@test ismatch(ir, "100.222")
@test ismatch(ir, "1.000.222")
@test ismatch(ir, "10.000.222")
@test ismatch(ir, "100.000.222")
@test ismatch(ir, "1.000.000.222")
@test ismatch(ir, "01.000.000.222")

fm = Tables.CSVFormat(decimal_separator='.', thousands_separator=Nullable(','))
ir = Tables.integer_regex(fm)
@test ismatch(ir, "0")
@test ismatch(ir, "-1")
@test !ismatch(ir, "am")
@test !ismatch(ir, "-1.23")
@test !ismatch(ir, "-1.23")
@test ismatch(ir, "123,123")
@test !ismatch(ir, "23,10,2015")
@test !ismatch(ir, "23.10.15")
@test !ismatch(ir, "1,2,3")
@test !ismatch(ir, "1.2.3")
@test ismatch(ir, "10")
@test ismatch(ir, "100")
@test !ismatch(ir, "1203")
@test ismatch(ir, "1,203")
@test ismatch(ir, "10,222")
@test ismatch(ir, "100,222")
@test ismatch(ir, "1,000,222")
@test ismatch(ir, "10,000,222")
@test ismatch(ir, "100,000,222")
@test ismatch(ir, "1,000,000,222")
@test !ismatch(ir, "1.000.000.222")
@test ismatch(ir, "01,000,000,222")

# valid floats without thousands separator
fm = Tables.CSVFormat(decimal_separator='.')
ir = Tables.float_regex(fm)
@test ismatch(ir, "0")
@test ismatch(ir, "1203")
@test ismatch(ir, "-1")
@test !ismatch(ir, "am")
@test !ismatch(ir, "-1,23")
@test ismatch(ir, "-1.23")
@test ismatch(ir, "123.123")
@test !ismatch(ir, "23.10.2015")
@test !ismatch(ir, "23.10.15")
@test !ismatch(ir, "1,2,3")
@test !ismatch(ir, "1.2.3")
@test ismatch(ir, "123123123.123123123123")
@test ismatch(ir, "123123123.")
@test ismatch(ir, ".0000")
@test !ismatch(ir, ".")
@test ismatch(ir, ".0")
@test ismatch(ir, "-.0")

# valid floats with thousands separator
fm = Tables.CSVFormat(thousands_separator=Nullable('.'))
ir = Tables.float_regex(fm)
@test ismatch(ir, "0")
@test ismatch(ir, "-1")
@test !ismatch(ir, "am")
@test ismatch(ir, "-1,23")
@test !ismatch(ir, "-1.23")
@test ismatch(ir, "123.123")
@test !ismatch(ir, "23.10.2015")
@test !ismatch(ir, "23.10.15")
@test !ismatch(ir, "1,2,3")
@test !ismatch(ir, "1.2.3")
@test ismatch(ir, "10")
@test ismatch(ir, "100")
@test !ismatch(ir, "1203")
@test ismatch(ir, "1.203")
@test ismatch(ir, "10.222")
@test ismatch(ir, "100.222")
@test ismatch(ir, "1.000.222")
@test ismatch(ir, "10.000.222")
@test ismatch(ir, "100.000.222")
@test ismatch(ir, "1.000.000.222")
@test ismatch(ir, "01.000.000.222")
@test ismatch(ir, "10,1")
@test ismatch(ir, "100,1")
@test !ismatch(ir, "1203,1123123123")
@test ismatch(ir, "1.203,1")
@test ismatch(ir, "10.222,1")
@test ismatch(ir, "100.222,1")
@test ismatch(ir, "1.000.222,1")
@test ismatch(ir, "10.000.222,1")
@test ismatch(ir, "-100.000.222,1")
@test ismatch(ir, "1.000.000.222,1")
@test ismatch(ir, "01.000.000.222,1")

# valid floats with thousands separator
fm = Tables.CSVFormat(decimal_separator='.', thousands_separator=Nullable(','))
ir = Tables.float_regex(fm)
@test ismatch(ir, "0")
@test ismatch(ir, "-1")
@test !ismatch(ir, "am")
@test !ismatch(ir, "-1,23")
@test ismatch(ir, "-1.23")
@test ismatch(ir, "123,123")
@test !ismatch(ir, "23.10.2015")
@test !ismatch(ir, "23.10.15")
@test !ismatch(ir, "1,2,3")
@test !ismatch(ir, "1.2.3")
@test ismatch(ir, "10")
@test ismatch(ir, "100")
@test !ismatch(ir, "1203")
@test ismatch(ir, "1,203")
@test ismatch(ir, "10,222")
@test ismatch(ir, "100,222")
@test ismatch(ir, "1,000,222")
@test ismatch(ir, "10,000,222")
@test ismatch(ir, "100,000,222")
@test ismatch(ir, "1,000,000,222")
@test ismatch(ir, "01,000,000,222")
@test ismatch(ir, "10.1")
@test ismatch(ir, "100.1")
@test !ismatch(ir, "1203.1123123123")
@test ismatch(ir, "1,203.1")
@test ismatch(ir, "10,222.1")
@test ismatch(ir, "100,222.1")
@test ismatch(ir, "1,000,222.1")
@test ismatch(ir, "10,000,222.1")
@test ismatch(ir, "-100,000,222.1")
@test ismatch(ir, "1,000,000,222.1")
@test ismatch(ir, "01,000,000,222.1")

fm = Tables.CSVFormat(thousands_separator=Nullable('.'), date_format=Dates.DateFormat("dd/mm/Y"))

raw = readdlm("example.csv", fm.dlm, String)

s = Tables.infer_type(raw[2,3], fm)
@test s == Tables.InferenceState(Int, false)
Tables.infer_type(raw[3,3], fm, s)
@test s == Tables.InferenceState(Float64,false)
Tables.infer_type(raw[5,3], fm, s)
@test s == Tables.InferenceState(Float64,false) # should not go back to Int

sch = Tables.infer_schema(raw, fm)
@test sch == Schema(Symbol[:COL_A,:COL_B,:COL_C,:COL_D], DataType[String,Int64,Float64,Date])

tb = Tables.readcsv("example.csv", sch, fm)

fm2 = Tables.CSVFormat(decimal_separator='.', date_format=Dates.DateFormat("dd/mm/Y"))
tb_copy = Tables.readcsv("example_no_ts.csv", fm2)
@test isequal(tb, tb_copy)

@test Tables.extract_nonempty_string("hey") == "hey"
@test Tables.extract_nonempty_string(" hey ") == "hey"
@test Tables.extract_nonempty_string("   hey     ") == "hey"
@test Tables.extract_nonempty_string("hey      ") == "hey"
@test Tables.extract_nonempty_string("    hey") == "hey"
@test Tables.extract_nonempty_string("hey you") == "hey you"
@test Tables.extract_nonempty_string("hey 2 you") == "hey 2 you"
@test Tables.extract_nonempty_string("hey 2\" you") == "hey 2\" you"
@test Tables.extract_nonempty_string("hey 2\" y\"ou") == "hey 2\" y\"ou"
@test Tables.extract_nonempty_string("\"hey 2\" y\"ou\"") == "hey 2\" y\"ou"

fm = Tables.CSVFormat(dlm=',', decimal_separator='.', )
tb = Tables.readcsv("example_nullable.csv", fm; header=false)
@test get(tb[1,1] == Nullable(1))
@test isnull(tb[2,1])
@test get(tb[3,1] == Nullable(3))
@test get(tb[1,2] == Nullable("Ab Cd"))
@test isnull(tb[2,2])
@test isnull(tb[3,2])
@test get(tb[1,3] == Nullable(100.5))
@test isnull(tb[2,3])
@test get(tb[3,3] == Nullable(2.0))

p = [:a => String, :b => Int]
s = Tables.Schema(p)
@test p == Tables.pairs(s)
