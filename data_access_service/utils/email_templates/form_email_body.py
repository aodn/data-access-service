"""The shared email body: every email type looks the same; only the intro
under "Hi," (and the optional download button) differs."""

from html import escape

from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.file_naming import dataset_base_name
from data_access_service.utils.email_templates.email_layout import (
    EMAIL_HEAD,
    EMAIL_HEADER,
    EMAIL_FOOTER,
)
from data_access_service.utils.email_templates.form_section_divs import (
    form_heading_div,
    form_text_div,
    form_paragraph,
)
from data_access_service.utils.email_templates.form_subsetting_divs import (
    form_subsetting_divs,
)

USAGE_CONSTRAINTS_NOT_AVAILABLE = "Usage Constraints not available"


def usage_constraint_paragraphs(usage_constraints: list[str]) -> list[str]:
    """The Usage Constraints paragraphs, as escaped HTML.

    Same as the portal dialog: an empty list shows "not available", never
    fixed IMOS text.
    """
    if not usage_constraints:
        return [USAGE_CONSTRAINTS_NOT_AVAILABLE]
    return [escape(item) for item in usage_constraints]


def form_intro_div(content_html: str) -> str:
    """The intro block under the banner; content_html is the per-email paragraphs."""
    return f"""                    <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;border-radius:16px 16px 0px 0px;max-width:568px;">
                        <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;border-radius:16px 16px 0px 0px;">
                            <tbody>
                            <tr>
                                <td style="border:none;direction:ltr;font-size:0;padding:24px 20px 4px 20px;text-align:center;">
                                    <!--[if mso | IE]>
                                    <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                        <tr>
                                            <td style="vertical-align:middle;width:528px;">
                                    <![endif]-->
                                    <div class="l h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                                        <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                                            <tbody>
                                            <tr>
                                                <td align="left" style="font-size:0;padding-bottom:24px;word-break:break-word;">
                                                    <div style="font-family:'Open Sans', 'Arial', sans-serif;font-size:16px;font-weight:400;line-height:150%;text-align:left;color:#090c02;">
                                                        {content_html}</div>
                                                </td>
                                            </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                    <!--[if mso | IE]>
                                    </td>
                                    </tr>
                                    </table>
                                    <![endif]-->
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
    <!--[if mso | IE]></td></tr></table></td></tr><![endif]-->"""


def form_sign_off_div() -> str:
    """The sign-off block ("Kind regards, ..."); identical in every email."""
    return """    <!--[if mso | IE]><tr><td width="600px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:568px;" width="568"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
                    <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;border-radius:0px 0px 16px 16px;max-width:568px;">
                        <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;border-radius:0px 0px 16px 16px;">
                            <tbody>
                            <tr>
                                <td style="border:none;direction:ltr;font-size:0;padding:24px 20px 40px 20px;text-align:center;">
                                    <!--[if mso | IE]>
                                    <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                        <tr>
                                            <td style="vertical-align:middle;width:528px;">
                                    <![endif]-->
                                    <div class="l h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                                        <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                                            <tbody>
                                            <tr>
                                                <td align="left" style="font-size:0;word-break:break-word;">
                                                    <div style="font-family:'Open Sans', 'Arial', sans-serif;font-size:16px;font-weight:400;line-height:150%;text-align:left;color:#090c02;">
                                                        <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">If you require assistance with this service please contact us with <span style="color:#2571e9;text-decoration:underline;"><a href="mailto:info@aodn.org.au" style="color:#2571e9;text-decoration:underline;">info@aodn.org.au</a></span>. </p>
                                                        <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">&nbsp;</p>
                                                        <p style="Margin:0;mso-line-height-alt:23px;font-size:16px;line-height:153%;"><span style="font-size:15px;color:#000000;line-height:153%;mso-line-height-alt:23px;">Kind regards,</span></p>
                                                        <p style="Margin:0;mso-line-height-alt:23px;font-size:16px;line-height:153%;"><span style="font-size:15px;color:#000000;line-height:153%;mso-line-height-alt:23px;">Australian Ocean Data Network</span></p>
                                                    </div>
                                                </td>
                                            </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                    <!--[if mso | IE]>
                                    </td>
                                    </tr>
                                    </table>
                                    <![endif]-->
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>"""


def form_email_html(
    subset_request: SubsetRequest, intro_content_html: str, button_html: str = ""
) -> str:
    """Assemble a full email around the per-email intro (and optional
    download button)."""

    collection_title = escape(subset_request.collection_title or "")
    metadata_link = subset_request.full_metadata_link
    if metadata_link:
        collection_title = (
            f'<a href="{escape(metadata_link, quote=True)}" '
            f'style="color:#2571e9;text-decoration:underline;">{collection_title}</a>'
        )
    collection_section = form_heading_div("Dataset:", top_padding=4) + form_text_div(
        form_paragraph(collection_title)
    )

    data_selection_section = form_heading_div("Data Selection:") + form_text_div(
        form_paragraph(
            "<br>".join(escape(dataset_base_name(key)) for key in subset_request.keys)
        )
    )

    format_section = form_heading_div("Format:") + form_text_div(
        form_paragraph((subset_request.output_format or "").upper())
    )

    subsetting_section = form_subsetting_divs(
        subset_request.start_date, subset_request.end_date, subset_request.multi_polygon
    )

    citation_section = form_heading_div("Suggested Citation:") + form_text_div(
        form_paragraph(subset_request.suggested_citation or "")
    )

    usage_constraints_section = form_heading_div("Usage Constraints:") + "".join(
        form_text_div(form_paragraph(text))
        for text in usage_constraint_paragraphs(subset_request.usage_constraints)
    )

    return "\n".join(
        [
            EMAIL_HEAD,
            EMAIL_HEADER,
            form_intro_div(intro_content_html),
            button_html,
            collection_section,
            data_selection_section,
            format_section,
            subsetting_section,
            citation_section,
            usage_constraints_section,
            form_sign_off_div(),
            EMAIL_FOOTER,
        ]
    )
