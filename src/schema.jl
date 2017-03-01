
function Base.:(==)(sa::TableSchema, sb::TableSchema)
    return (sa.names == sb.names) && (sa.types == sb.types)
end

# shallow-copy
function Base.copy(s::TableSchema)
    new_header = copy(s.names)
    new_types = copy(s.types)
    return TableSchema(new_header, new_types)
end

function Base.deepcopy(s::TableSchema)
    new_header = deepcopy(s.names)
    new_types = deepcopy(s.types)
    return TableSchema(new_header, new_types)
end

function TableSchema(header::Vector, types::Vector{DataType})
    header = [Symbol(x) for x in header]
    return TableSchema(header, types)
end

# Allow for syntax: TableSchema( [:a => String, :b => Int] )
function TableSchema(v::Vector{Pair{Symbol, DataType}})
    n = length(v)
    head = Vector{Symbol}(n)
    types = Vector{DataType}(n)

    for i in 1:n
        head[i] = v[i][1]
        types[i] = v[i][2]
    end

    return TableSchema(head, types)
end

# Returns [ :a => String, :b => Int ]
function pairs(s::TableSchema)
    n = ncol(s)
    result = Vector{Pair{Symbol, DataType}}(n)

    for i in 1:n
        result[i] = s.names[i] => s.types[i]
    end
    return result
end

# Allow syntax: TableSchema(a=String, b=Int)
function TableSchema(; kwargs...)
    n = length(kwargs)
    fields = Vector{TableField}(n)
    for i in 1:n
        k, v = kwargs[i]
        fields[i] = TableField(k, isa(v, DataType) ? v : eltype(v))
    end

    return TableSchema(fields)
end

function Base.push!(s::TableSchema, column_description::Pair{Symbol, DataType})
    push!(s.names, column_description[1])
    push!(s.types, column_description[2])
end

DataFrames.names(s::TableSchema) = s.names

function DataFrames.names!(s::TableSchema, new_names::Vector{Symbol})
    s.names = new_names
end

DataFrames.ncol(s::TableSchema) = length(s.names)
