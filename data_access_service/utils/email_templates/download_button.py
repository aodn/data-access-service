from data_access_service.utils.email_templates.email_images import (
    DOWNLOAD_ICON,
)


def generate_download_button_html(object_urls):
    """
    Generate download button HTML only for single URL.
    Multiple URLs: Returns empty string (hides the button)
    """
    if len(object_urls) == 1:
        object_url_str = object_urls[0]
        return f"""
        <tr>
            <td align="center" class="b" style="font-size:0;padding:0;padding-bottom:0;word-break:break-word;">
                <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border-collapse:separate;width:244px;line-height:100%;">
                    <tbody>
                    <tr>
                        <td align="center" bgcolor="#3b6e8f" class="co" role="presentation" style="background:#3b6e8f;border:none;border-radius:6px 6px 6px 6px;cursor:auto;mso-padding-alt:12px 0px 12px 0px;vertical-align:middle;" valign="middle">
                            <!--[if mso]>
                            <v:roundrect style="width:244px;height:38px;v-text-anchor:middle;" arcsize="32%" fill="t" stroke="f" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:w="urn:schemas-microsoft-com:office:word">
                                <w:anchorlock/>
                                <v:fill type="solid" color="#3b6e8f" />
                                <v:textbox inset="0,0,0,0">
                                <center>
                                    <![endif]-->
                                    <a href="{object_url_str}" class="co" style="display:inline-block;width:244px;background-color:#3b6e8f;color:#ffffff;font-family:'Open Sans', 'Arial', sans-serif;font-size:13px;font-weight:normal;line-height:100%;margin:0;text-decoration:none;text-transform:none;padding:12px 0px 12px 0px;mso-padding-alt:0;border-radius:6px 6px 6px 6px;" target="_blank">
                                    <span style="display:inline;mso-hide:all;vertical-align:middle;"><img src="data:image/png;base64,{DOWNLOAD_ICON}" width="22" height="22" style="display:inline;mso-hide:all;vertical-align:middle;width:22px;height:22px;Margin-right:8px;"></span><span style="vertical-align:middle;font-size:16px;font-family:'Open Sans', 'Arial', sans-serif;font-weight:500;color:#ffffff;line-height:150%;mso-line-height-alt:24px;">Download</span>
                                    </a>
                                    <!--[if mso]>
                                </center>
                                </v:textbox>
                            </v:roundrect>
                            <![endif]-->
                        </td>
                    </tr>
                    </tbody>
                </table>
            </td>
        </tr>
        """
    else:
        # Multiple URLs: hide the button (return empty string)
        return ""
