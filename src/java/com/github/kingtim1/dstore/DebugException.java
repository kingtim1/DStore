package com.github.kingtim1.dstore;

/**
 * Thrown to indicate something that happened that should never happen when the
 * program is functioning correctly.
 * 
 * @author Timothy Mann
 * 
 */
public class DebugException extends RuntimeException
{

	private static final long serialVersionUID = 7611682682846809791L;

	/**
	 * Constructs a debug exception with a message.
	 * 
	 * @param message
	 */
	public DebugException(String message)
	{
		super(message);
	}

	public DebugException()
	{
		super();
	}

	public DebugException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public DebugException(Throwable cause)
	{
		super(cause);
	}
	
	
}
