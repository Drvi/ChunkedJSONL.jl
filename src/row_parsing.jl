function ChunkedBase.populate_result_buffer!(
    result_buf::TaskResultBuffer,
    newlines_segment::AbstractVector{Int32},
    parsing_ctx::ParsingContext,
    buf::Vector{UInt8},
    comment::Union{Nothing,Vector{UInt8}}=nothing,
    ::Type{CT}=Tuple{}
) where {CT}
    tape = result_buf.tape
    empty!(result_buf)
    Base.ensureroom(result_buf, ceil(Int, length(newlines_segment) * 1.01))
    ignoreemptyrows = parsing_ctx.ignoreemptyrows

    tapeidx = 1
    @inbounds for i in 1:length(newlines_segment) - 1
        pos = Int(newlines_segment[i]) + 1
        len = Int(newlines_segment[i+1]) - 1
        # skip over leading spaces
        ChunkedBase._startswith(buf, pos - 1, comment) && continue
        _nonspace(buf[pos]) || (pos = something(findnext(_nonspace, buf, pos), -1))
        ignoreemptyrows && (pos == -1 || pos > len) && continue

        JSON3.@check
        unsafe_push!(result_buf.tapeidxs, tapeidx)
        _, tapeidx = JSON3.read!(buf, pos, len, buf[pos], tape, tapeidx, Any)
    end
    return nothing
end
