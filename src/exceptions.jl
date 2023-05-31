abstract type FatalLexingError <: Exception end
# Base.showerror(io::IO, e::FatalLexingError) = print(io, e.msg)

# # TODO: Add some data to help debug the problematic file, like the first row with an escape character
# #       and/or the quote character.
# struct NoValidRowsInBufferError <: FatalLexingError
#     msg::String
#     buffersize::UInt32

#     function NoValidRowsInBufferError(buffersize::UInt32)
#         return new(
#             string(
#                 "JSONLines parse job failed on lexing newlines. There was no linebreak in the entire buffer ",
#                 "of $(buffersize) bytes. This can happen when `buffersize` is small relative to size of any ",
#                 "given JSON Line or if the file is malformed and contains newline characters inside the individual ",
#                 "JSON objects or arrays.",
#             ),
#             buffersize,
#         )
#     end
# end
