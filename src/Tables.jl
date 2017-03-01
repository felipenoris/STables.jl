
__precompile__(true)
module Tables

using NullableArrays
using DataFrames
using Lifting
import AbstractTables

include("grisu.jl")
include("types.jl")
include("schema.jl")
include("table.jl")
include("tablerow.jl")
include("schema_inference.jl")
include("csv.jl")

export Table, TableSchema, TableField

end # module Tables
