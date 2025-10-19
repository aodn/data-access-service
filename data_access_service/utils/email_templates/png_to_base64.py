import base64


def png_to_base64(image_path):
    """
    Converts a PNG image file to a Base64 encoded string.

    Args:
        image_path (str): The path to the PNG image file.

    Returns:
        str: The Base64 encoded string of the image, or None if an error occurs.

    How to use it in the html email:
    <img alt src="data:image/png;base64,your_base64_code" style="border:0;display:block;outline:none;text-decoration:none;height:auto;width:100%;font-size:13px;" width="344">
    """
    try:
        with open(image_path, "rb") as image_file:
            # Read the binary data of the image
            binary_image_data = image_file.read()
            # Encode the binary data to Base64
            base64_encoded_bytes = base64.b64encode(binary_image_data)
            # Decode the bytes to a UTF-8 string for easier handling
            base64_string = base64_encoded_bytes.decode("utf-8")
            return base64_string
    except FileNotFoundError:
        print(f"Error: The file '{image_path}' was not found.")
        return None
    except Exception as e:
        print(f"An error occurred during conversion: {e}")
        return None
