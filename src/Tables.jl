
module Tables

@assert VERSION >= v"0.7-" "Tables.jl requires Julia v0.7.0 or newer."

using DataFrames
using Dates
import DelimitedFiles

include("grisu.jl")
include("types.jl")
include("schema.jl")
include("table.jl")
include("tablerow.jl")
include("schema_inference.jl")
include("csv.jl")

export Table, Schema

end # module Tables
