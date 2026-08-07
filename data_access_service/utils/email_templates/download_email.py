from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.email_templates.download_button import (
    generate_download_button_html,
)
from data_access_service.utils.email_templates.form_email_body import form_email_html
from data_access_service.utils.email_templates.form_section_divs import form_paragraph
from data_access_service.utils.email_templates.no_data_email import (
    get_no_data_email_html_body,
)


def get_download_email_html_body(
    subset_request: SubsetRequest, object_urls: [str]
) -> str:
    """The "download ready" email: the shared body with its own intro and a
    download button."""

    if not object_urls or len(object_urls) == 0:
        return get_no_data_email_html_body(subset_request)

    object_url_str = "<br>".join(
        [
            f'<a href="{url}" style="color:#2571e9;text-decoration:underline;">{url}</a>'
            for url in object_urls
        ]
    )
    intro = (
        form_paragraph("Hi,")
        + form_paragraph("&nbsp;")
        + form_paragraph(
            "Your AODN data download request has been completed. "
            'Please click on the following link to access your data request.<br class="s">'
            + object_url_str
        )
        + form_paragraph(
            "The download will be available for 7 days.", margin="8px 0 0 0"
        )
    )
    return form_email_html(
        subset_request, intro, generate_download_button_html(object_urls)
    )
