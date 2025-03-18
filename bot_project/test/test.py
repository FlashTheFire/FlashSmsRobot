def build_simple_advanced_query(user_input: str) -> str:
    """
    Build an advanced fuzzy query for the @app_name field.
    
    The function permanently removes spaces from the input and then creates the following variants:
      1. Variant with %% substring wrapper: "%%<input>%%"
      2. Variant with a trailing wildcard: "<input>*"
      3. Variant with a leading wildcard: "*<input>"
      4. Variant with wildcards on both sides: "*<input>*"
      5. The original input (with spaces removed)
      
    All variants are joined with the OR operator (|) and the final query is wrapped as:
      @app_name:(<variant1>|<variant2>|...|<variant5>)
      
    If the query is empty or only spaces, the function returns: @app_name:*
    """
    # Remove spaces permanently and convert to lower-case.
    processed = user_input.strip().lower().replace(" ", "")
    
    if not processed:
        return "@app_name:*"
    
    variant1 = "%%" + processed + "%%"   # Using %% wrapper for substring matching.
    variant2 = processed + "*"            # Trailing wildcard.
    variant3 = "*" + processed            # Leading wildcard.
    variant4 = "*" + processed + "*"      # Both sides wildcards.
    variant5 = processed                  # Original processed input.
    
    # Combine variants using the OR operator.
    or_clause = f"{variant1}|{variant2}|{variant3}|{variant4}|{variant5}"
    return f"@app_name:({or_clause})"

# Example usage:
if __name__ == '__main__':
    # Example 1: Input with spaces (which will be permanently removed).
    query1 = build_simple_advanced_query("tata ne")
    print("Query 1:", query1)
    # Expected output: @app_name:(%%tataneu%%|tataneu*|*tataneu|*tataneu*|tataneu)
    
    # Example 2: Input without spaces.
    query2 = build_simple_advanced_query("tatneu")
    print("Query 2:", query2)
    # Expected output: @app_name:(%%tataneu%%|tataneu*|*tataneu|*tataneu*|tataneu)
    
    # Example 3: Empty query.
    query3 = build_simple_advanced_query("   ")
    print("Query 3:", query3)
    # Expected output: @app_name:*
