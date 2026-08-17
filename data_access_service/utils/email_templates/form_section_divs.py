"""Reusable email blocks: section headings, body text and paragraphs.
Each block is self-contained, so they can be stacked in any order."""


def form_paragraph(text: str, margin: str = "0") -> str:
    """A body paragraph in the email's standard 16px style."""
    return f'<p style="Margin:{margin};mso-line-height-alt:24px;font-size:16px;line-height:150%;">{text}</p>'


def form_heading_div(title: str, top_padding: int = 16) -> str:
    """A 17px section heading (e.g. "Collection") as a full-width row."""
    return f"""
    <!--[if mso | IE]><tr><td width="600px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:568px;" width="568"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
    <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:568px;">
        <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
            <tbody>
            <tr>
                <td style="border:none;direction:ltr;font-size:0;padding:{top_padding}px 20px 4px 20px;text-align:center;">
                    <!--[if mso | IE]>
                    <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                        <tr>
                            <td style="vertical-align:middle;width:528px;">
                    <![endif]-->
                    <div class="l h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                        <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                            <tbody>
                            <tr>
                                <td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
                                    <table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
                                        <tr>
                                            <td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="auto">
                                                <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                                    <tr>
                                                        <td align="left" width="100%">
                                                            <div style="font-family: 'Open Sans', 'Arial', sans-serif; font-size: 17px; font-weight: 500; line-height: 141%; text-align: left; color: #090c02">
                                                                <p style="Margin:0;mso-line-height-alt:24px;font-size:17px;line-height:141%;">{title}</p>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                </table>
                                            </td>
                                        </tr>
                                    </table>
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


def form_text_div(content_html: str) -> str:
    """A 16px body block under a section heading; content_html is one or
    more paragraphs (see form_paragraph)."""
    return f"""
    <!--[if mso | IE]><tr><td width="600px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:568px;" width="568"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
    <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:568px;">
        <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
            <tbody>
            <tr>
                <td style="border:none;direction:ltr;font-size:0;padding:4px 20px 4px 20px;text-align:center;">
                    <!--[if mso | IE]>
                    <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                        <tr>
                            <td style="vertical-align:middle;width:528px;">
                    <![endif]-->
                    <div class="l h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                        <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                            <tbody>
                            <tr>
                                <td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
                                    <table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
                                        <tr>
                                            <td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="528">
                                                <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                                    <tr>
                                                        <td align="left" width="100%">
                                                            <div style="font-family: 'Open Sans', 'Arial', sans-serif; font-size: 16px; font-weight: 400; line-height: 150%; text-align: left; color: #090c02">
                                                                {content_html}
                                                            </div>
                                                        </td>
                                                    </tr>
                                                </table>
                                            </td>
                                        </tr>
                                    </table>
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
