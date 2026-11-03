from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.email_templates.form_email_body import form_email_html
from data_access_service.utils.email_templates.form_section_divs import form_paragraph

ERROR_EMAIL_SUBJECT = "Error Processing Your Download Request"


def get_error_email_html_body(subset_request: SubsetRequest) -> str:
    """The download error email: the shared body with its own intro."""
    intro = (
        form_paragraph("Hi,")
        + form_paragraph("&nbsp;")
        + form_paragraph(
            "Unfortunately, an error occurred while processing your download request. "
            "Please try again. If the problem persists, please contact us at "
            '<a href="mailto:info@aodn.org.au" '
            'style="color:#2571e9;text-decoration:underline;">info@aodn.org.au</a> '
            "for assistance."
        )
    )
    return form_email_html(subset_request, intro)
