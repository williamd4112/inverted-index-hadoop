package calculateAverage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.StringTokenizer;

abstract public class BooleanLogicParser {
	
	final private Map<String, Integer> PRIORITYS = Collections.unmodifiableMap(
		    new HashMap<String, Integer>() {
		    	/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

			{
		    	put("(", 0);
		    	put("|", 2);
		    	put("&", 3);
		    	put("!", 4);
		    	put(")", 5);
		    }});
	
	final private Map<String, Token> TOKEN_TABLE = Collections.unmodifiableMap(
		    new HashMap<String, Token>() {
		    	/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

			{
		    	put("|", Token.OR);
		    	put("&", Token.AND);
		    	put("!", Token.NOT);
		    }});
	
	public enum Token
	{
		AND, OR, NOT, TRUE, FALSE
	}
	
	private ArrayList<Token> postifxExpression = new ArrayList<Token>();
	
	public boolean eval() throws BooleanLogicParserException
	{
		Stack<Token> parseStack = new Stack<Token>();
		for(Token token : this.postifxExpression)
		{
			Token op1, op2;
			switch(token)
			{
			case AND:
				op1 = parseStack.pop();
				op2 = parseStack.pop();
				parseStack.push(and(op1, op2));
				break;
			case OR:
				op1 = parseStack.pop();
				op2 = parseStack.pop();
				parseStack.push(or(op1, op2));
				break;
			case NOT:
				op1 = parseStack.pop();
				parseStack.push(not(op1));
				break;
			case TRUE: case FALSE:
				parseStack.push(token);
				break;
			default:
				throw new BooleanLogicParserException();
			}
		}
		
		if(parseStack.size() != 1)
			throw new BooleanLogicParserException();
		
		Token top = parseStack.pop();
		if(top != Token.TRUE && top != Token.FALSE)
			throw new BooleanLogicParserException();
		
		return top == Token.TRUE ? true : false;
	}
	
	public void parse(String input) throws BooleanLogicParserException 
	{
		this.postifxExpression.clear();
		
		Stack<String> tokenStack = new Stack<String>();
		StringTokenizer tokenizer = new StringTokenizer(input);
		
		while(tokenizer.hasMoreTokens())
		{
			String token = tokenizer.nextToken();
			int tp = getPriority(token);
			if(tp == 1)
				this.postifxExpression.add(evalID(token));
			else
			{
				if(token.equals(")"))
				{
					while(!tokenStack.isEmpty() && !tokenStack.peek().equals("("))
						popAndOutput(tokenStack);
				}
				else if(token.equals("("))
					tokenStack.add(token);
				else
				{
					while(!tokenStack.isEmpty() && tp < getTopPriority(tokenStack))
						popAndOutput(tokenStack);
				}
				tokenStack.push(token);
			}
		}
		
		while(!tokenStack.empty())
		{
			popAndOutput(tokenStack);
		}
	}
	
	private Token and(Token op1, Token op2) throws BooleanLogicParserException
	{	
		return (evalToken(op1) && evalToken(op2)) ? 
				Token.TRUE : Token.FALSE;
	}
	
	private Token not(Token op) throws BooleanLogicParserException
	{
		return !(evalToken(op)) ? 
				Token.TRUE : Token.FALSE;
	}
	
	private Token or(Token op1, Token op2) throws BooleanLogicParserException
	{
		return (evalToken(op1) || evalToken(op2)) ? 
				Token.TRUE : Token.FALSE;
	}
	
	abstract protected Token evalID(String id);

	private void popAndOutput(Stack<String> tokenStack) throws BooleanLogicParserException 
	{
		String toPop = tokenStack.pop();
		if(!toPop.equals("(") && !toPop.equals(")"))
			this.postifxExpression.add(evalTokenString(toPop));
	}
	
	private Token evalTokenString(String toPop) throws BooleanLogicParserException 
	{
		Token token = TOKEN_TABLE.get(toPop);
		if(token == null)
			throw new BooleanLogicParserException();
		return token;
	}
	
	private boolean evalToken(Token token) throws BooleanLogicParserException
	{
		switch(token)
		{
		case TRUE:
			return true;
		case FALSE:
			return false;
		default:
			throw new BooleanLogicParserException();
		}
	}

	private int getTopPriority(Stack<String> tokenStack)
	{
		return getPriority(tokenStack.peek());
	}
	
	private int getPriority(String token)
	{
		Integer priority = PRIORITYS.get(token);
		return (priority == null) ? 1 : priority.intValue();
	}

}
