from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.email_templates.form_email_body import form_email_html
from data_access_service.utils.email_templates.form_section_divs import form_paragraph

NO_DATA_EMAIL_SUBJECT = "No Data Available for Your Subset Request"


def get_no_data_email_html_body(subset_request: SubsetRequest) -> str:
    """The "no data available" email: the shared body with its own intro."""
    intro = (
        form_paragraph("Hi,")
        + form_paragraph("&nbsp;")
        + form_paragraph(
            "No data available for the requested subset of the dataset collection."
        )
    )
    return form_email_html(subset_request, intro)
