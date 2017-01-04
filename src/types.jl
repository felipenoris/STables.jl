
type Schema
    names::Vector{Symbol}       # column names
    types::Vector{DataType}      # Julia types of columns

    function Schema(header::Vector{Symbol}, types::Vector{DataType})
        @assert length(header) == length(types) "Sizes mismatch: header = $(length(header)), types = $(length(types))"
        @assert length(header) == length(unique(header)) "Column names must be unique"
        new(header, types)
    end
end

type Table #<: AbstractDataFrame
    schema::Schema
    data::Vector{Any}

    function Table(s::Schema, d::Vector{Any})
        @assert length(s.names) == length(d)
        for i = 1:length(s.types)
            @assert eltype(d[i]) == s.types[i] "Vector types don't match table's schema."
        end
        new(s,d)
    end
end

immutable TableRow
    table::Table
    row::Int
end

immutable TableRowIterator
    table::Table
end
