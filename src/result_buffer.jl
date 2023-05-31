struct TaskResultBuffer <: AbstractResultBuffer
    id::Int
    tape::Vector{UInt64}
    tapeidxs::BufferedVector{Int}
end

TaskResultBuffer(id::Int) = TaskResultBuffer(id, UInt64[], BufferedVector{Int}())
TaskResultBuffer(id::Int, n) = TaskResultBuffer(id, UInt64[], BufferedVector{Int}(Vector{Int}(undef, n), 0))


function Base.empty!(buf::TaskResultBuffer)
    empty!(buf.tapeidxs)
    empty!(buf.tape)
    return buf
end

function Base.ensureroom(buf::TaskResultBuffer, n)
    Base.ensureroom(buf.tapeidxs, n)
    return buf
end
