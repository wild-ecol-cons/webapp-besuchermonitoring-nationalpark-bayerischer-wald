import streamlit as st
import hmac

def check_password(
        type_of_password: str
    ) -> bool:
    """
    Checks whether the password entered by the user is as expected.

    Args:
        type_of_password (str): The type of password (e.g., "test" or "admin").

    Returns:
        bool: True if the password is correct, False otherwise.
    """

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if hmac.compare_digest(st.session_state[type_of_password], st.secrets[f"{type_of_password}_password"]):
            st.session_state[f"{type_of_password}_correct"] = True
            del st.session_state[type_of_password]  # Don't store the password.
        else:
            st.session_state[f"{type_of_password}_correct"] = False

    # Return True if the password is validated.
    if st.session_state.get(f"{type_of_password}_correct", False):
        return True

    # Show input for password.
    st.text_input(
        "Password", type="password", on_change=password_entered, key=type_of_password
    )
    if f"{type_of_password}_correct" in st.session_state:
        st.error("😕 Password incorrect")
    return False
