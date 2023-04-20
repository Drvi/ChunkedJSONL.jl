function _parse_file_serial(lexer_state::LexerState, parsing_ctx::ParsingContext, consume_ctx::AbstractConsumeContext)
    row_num = UInt32(1)
    result_buf = TaskResultBuffer(0, length(parsing_ctx.eols))
    try
        while true
            task_size = estimate_task_size(parsing_ctx)
            task_start = UInt32(1)
            for task in Iterators.partition(eachindex(parsing_ctx.eols), task_size)
                setup_tasks!(consume_ctx, parsing_ctx, 1)
                task_end = UInt32(last(task))
                _parse_rows_forloop!(result_buf, view(parsing_ctx.eols, task_start:task_end), parsing_ctx.bytes)
                consume!(consume_ctx, parsing_ctx, result_buf, row_num, task_start)
                row_num += task_end - task_start
                task_start = task_end
                task_done!(consume_ctx, parsing_ctx, result_buf)
                sync_tasks(consume_ctx, parsing_ctx, 1)
            end
            lexer_state.done && break
            read_and_lex!(lexer_state, parsing_ctx)
        end # while true
    catch e
        @lock parsing_ctx.cond.cond_wait begin
            isnothing(parsing_ctx.cond.exception) && (parsing_ctx.cond.exception = e)
            notify(parsing_ctx.cond.cond_wait, e, all=true, error=true)
        end
        cleanup(consume_ctx, e)
        rethrow(e)
    end
    return nothing
end
