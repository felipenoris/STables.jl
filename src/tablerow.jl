
Base.getindex(tr::TableRow, c::Int) = getindex(tr.table, tr.row, c)
Base.getindex(tr::TableRow, column_name::Symbol) = getindex(tr.table, tr.row, column_name)
Base.setindex!(tr::TableRow, value, c::Int) = setindex!(tr.table, value, tr.row, c)
Base.setindex!(tr::TableRow, value, colname::Symbol) = setindex!(tr.table, value, colname, tr.row)
Base.length(tr::TableRow) = length(tr.table.data)
DataFrames.eachrow(tb::Table) = TableRowIterator(tb)

function Base.iterate(itr::TableRowIterator)
    if isempty(itr.table)
        return nothing
    end

    return TableRow(itr.table, 1), 1
end

function Base.iterate(itr::TableRowIterator, current_row::Integer)
    next_row = current_row + 1
    if next_row > DataFrames.nrow(itr.table)
        return nothing
    else
        return TableRow(itr.table, next_row), next_row
    end
end

function Base.collect(row::TableRow)
    out = Array{Any}(undef, ncol(row.table))
    @inbounds for c in 1:ncol(row.table)
        out[c] = row.table[row.row, c]
    end
    return out
end

Base.getindex(row::TableRow, ::Colon) = collect(row)
