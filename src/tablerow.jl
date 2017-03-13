
Base.getindex(tr::TableRow, c::Int) = getindex(tr.table, tr.row, c)
Base.getindex(tr::TableRow, column_name::Symbol) = getindex(tr.table, tr.row, column_name)
Base.setindex!(tr::TableRow, value, c::Int) = setindex!(tr.table, value, tr.row, c)
Base.setindex!(tr::TableRow, value, colname::Symbol) = setindex!(tr.table, value, colname, tr.row)
Base.length(tr::TableRow) = length(tr.table.data)
DataFrames.eachrow(tb::Table) = TableRowIterator(tb)
Base.start(itr::TableRowIterator) = 1
Base.done(itr::TableRowIterator, s) = s > length(itr.table.data[1])
Base.next(itr::TableRowIterator, s) = ( TableRow(itr.table, s), s+1 )

function Base.collect(row::TableRow)
	out = Array{Any}(ncol(row.table))
	for c in 1:ncol(row.table)
		out[c] = row.table[row.row, c]
	end
	return out
end
