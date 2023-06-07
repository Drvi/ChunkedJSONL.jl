struct DebugContext <: AbstractConsumeContext; end

function ChunkedBase.consume!(consume_ctx::DebugContext, payload::ParsedPayload)
    task_buf = payload.results
    io = IOBuffer()
    write(io, string("Start row: ", payload.row_num, ", nrows: ", length(task_buf.tapeidxs), ", $(Base.current_task()) "))
    printstyled(IOContext(io, :color => true), "❚", color=Int(hash(Base.current_task()) % UInt8))
    println(io)
    @info String(take!(io))
    return nothing
end
struct ValueExtractionContext <: AbstractConsumeContext
    elements::Vector{Union{Dict{Symbol},Vector,String,Nothing,Bool,Float64,Int}}
    indices::Vector{Int}
    lock::ReentrantLock
end
ValueExtractionContext() = ValueExtractionContext([], Int[], ReentrantLock())
function Base.sort!(ctx::ValueExtractionContext)
    ctx.elements .= @view ctx.elements[sortperm(ctx.indices)]
    ctx.indices .= 1:length(ctx.indices)
    return ctx
end

function ChunkedBase.consume!(consume_ctx::ValueExtractionContext, payload::ParsedPayload)
    tape = payload.results.tape
    buf = payload.chunking_ctx.bytes
    row = Int(payload.row_num)
    @inbounds for tapeidx in payload.results.tapeidxs
        t = tape[tapeidx]
        val = JSON3.getvalue(Any, buf, tape, tapeidx, t)
        if isa(val, Union{JSON3.Object,JSON3.Array})
            val = copy(val)
        end
        lock(consume_ctx.lock)
        try
            push!(consume_ctx.elements, val)
            push!(consume_ctx.indices, row)
        finally
            unlock(consume_ctx.lock)
        end
        row += 1
    end
    return nothing
end
