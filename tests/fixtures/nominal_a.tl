local record A
    record Token
        e: integer
    end
end

function A.make(): A.Token
    return {e = 1}
end

return A
