abstract type AbstractConsumeContext end
abstract type AbstractTaskLocalConsumeContext <: AbstractConsumeContext end

function setup_tasks! end
function consume! end
function task_done! end
function sync_tasks end
function cleanup end

maybe_deepcopy(x::AbstractConsumeContext) = x
maybe_deepcopy(x::AbstractTaskLocalConsumeContext) = deepcopy(x)

struct DebugContext <: AbstractConsumeContext; end

function consume!(consume_ctx::DebugContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer, row_num::UInt32, eol_idx::UInt32)
    io = IOBuffer()
    write(io, string("Start row: ", row_num, ", nrows: ", length(task_buf.tapeidxs), ", $(Base.current_task()) "))
    printstyled(IOContext(io, :color => true), "❚", color=Int(hash(Base.current_task()) % UInt8))
    println(io)
    @info String(take!(io))
    return nothing
end


function setup_tasks!(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, ntasks::Int)
    @lock parsing_ctx.cond.cond_wait begin
        parsing_ctx.cond.ntasks = ntasks
    end
end
function task_done!(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, result_buf::TaskResultBuffer)
    @lock parsing_ctx.cond.cond_wait begin
        parsing_ctx.cond.ntasks -= 1
        notify(parsing_ctx.cond.cond_wait)
    end
end
function sync_tasks(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, ntasks::Int)
    @lock parsing_ctx.cond.cond_wait begin
        while true
            parsing_ctx.cond.exception !== nothing && rethrow(parsing_ctx.cond.exception)
            parsing_ctx.cond.ntasks == 0 && break
            wait(parsing_ctx.cond.cond_wait)
        end
    end
end
cleanup(consume_ctx::AbstractConsumeContext, e::Exception) = nothing

struct SkipContext <: AbstractConsumeContext
    SkipContext() = new()
end
function consume!(consume_ctx::SkipContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer, row_num::UInt32, eol_idx::UInt32)
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

function consume!(consume_ctx::ValueExtractionContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer, row_num::UInt32, eol_idx::UInt32)
    tape = task_buf.tape
    buf = parsing_ctx.bytes
    row = Int(row_num)
    @inbounds for tapeidx in task_buf.tapeidxs
        t = tape[tapeidx]
        val = JSON3.getvalue(Any, buf, tape, tapeidx, t)
        if isa(val, Union{JSON3.Object,JSON3.Array})
            val = copy(val)
        end
        @lock consume_ctx.lock begin
            push!(consume_ctx.elements, val)
            push!(consume_ctx.indices, row)
        end
        row += 1
    end
    return nothing
end
