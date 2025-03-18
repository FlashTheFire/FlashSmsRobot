ADMIN_ID = 1234567890

def require_admin(user_id: int) -> bool:
    """Check if user_id is equal to admin id."""
    return user_id == ADMIN_ID
