function _parse_rows_forloop!(result_buf::TaskResultBuffer, task::AbstractVector{UInt32}, buf)
    tape = result_buf.tape
    empty!(result_buf)
    Base.ensureroom(result_buf, ceil(Int, length(task) * 1.01))

    pos = Int(first(task)) + 1
    tapeidx = 1
    @inbounds for i in 1:length(task) - 1
        len = Int(task[i+1])
        JSON3.@check
        unsafe_push!(result_buf.tapeidxs, UInt32(tapeidx))
        pos, tapeidx = JSON3.read!(buf, pos, len, buf[pos], tape, tapeidx, Any)
        pos += 1
    end
    return nothing
end
