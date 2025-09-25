from data_access_service.utils.email_templates.png_to_base64 import png_to_base64

# <!-- Download Email -->
def get_download_email_html(uuid: str, object_url: str, conditions: str) -> str:
    header_img=png_to_base64("data_access_service/utils/email_templates/img/fb1ca2d6ea710e523d5430e8b4c97aee.png")
    download_icon=png_to_base64("data_access_service/utils/email_templates/img/e58a6cfdb179246fe86eb69b984294cf.png")
    bbox_img=png_to_base64("data_access_service/utils/email_templates/img/1419180c04931cd1e419efce2690d5b8.png")
    time_range_img=png_to_base64("data_access_service/utils/email_templates/img/6a9d0202f46342bc6bc7c3998cfa42b7.png")
    attributes_img=png_to_base64("data_access_service/utils/email_templates/img/b274dfda8abcd8b25fba336b3005aea1.png")
    footer_text_img=png_to_base64("data_access_service/utils/email_templates/img/72f4fdbff6ad47302caf554e4ed9d49d.png")
    footer_action_img=png_to_base64("data_access_service/utils/email_templates/img/e17deb6304ba92d747550b727f2b0b3c.png")
    facebook_img=png_to_base64("data_access_service/utils/email_templates/img/9a5ec6e59d87571e85c9dee34369050e.png")
    instagram_img=png_to_base64("data_access_service/utils/email_templates/img/61d7bf2cff5309382bd988da59d406db.png")
    butterfly_img=png_to_base64("data_access_service/utils/email_templates/img/e2ae815b8ce719a4026e08f7fb334b71.png")
    linkedin_img=png_to_base64("data_access_service/utils/email_templates/img/82192244cf3677674fda6af8c471897d.png")
    return f"""
<!doctype html>
<html lang="en" dir="auto" xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
<title></title>
<!--[if !mso]><!-->
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<!--<![endif]-->
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style type="text/css">

#outlook a {{ padding:0; }}
body {{ margin:0;padding:0;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%; }}
table, td {{ border-collapse:collapse;mso-table-lspace:0pt;mso-table-rspace:0pt; }}
img {{ border:0;height:auto;line-height:100%; outline:none;text-decoration:none;-ms-interpolation-mode:bicubic; }}
p {{ display:block;margin:13px 0; }}
</style>
<!--[if mso]>
<noscript>
<xml>
<o:OfficeDocumentSettings>
<o:AllowPNG/>
<o:PixelsPerInch>96</o:PixelsPerInch>
</o:OfficeDocumentSettings>
</xml>
</noscript>
<![endif]-->
<!--[if lte mso 11]>
<style type="text/css">

.h {{ width:100% !important; }}
</style>
<![endif]-->
<!--[if !mso]><!-->
<link href="https://fonts.googleapis.com/css?family=Open Sans:400,500" rel="stylesheet" type="text/css">
<!--<![endif]-->
<style type="text/css">

@media only screen and (min-width:1279px) {{
.hn {{ width:1248px !important; max-width: 1248px; }}
.h7 {{ width:308px !important; max-width: 308px; }}
.c {{ width:600px !important; max-width: 600px; }}
.o {{ width:100% !important; max-width: 100%; }}
.ci {{ width:1216px !important; max-width: 1216px; }}
}}
</style>
<style media="screen and (min-width:1279px)">
.moz-text-html .hn {{ width:1248px !important; max-width: 1248px; }}
.moz-text-html .h7 {{ width:308px !important; max-width: 308px; }}
.moz-text-html .c {{ width:600px !important; max-width: 600px; }}
.moz-text-html .o {{ width:100% !important; max-width: 100%; }}
.moz-text-html .ci {{ width:1216px !important; max-width: 1216px; }}
</style>
<style type="text/css">

@media only screen and (max-width:1278px) {{
table.m {{ width: 100% !important; }}
td.m {{ width: auto !important; }}
}}
</style>
<style type="text/css">

body {{
-webkit-font-smoothing:antialiased;
-moz-osx-font-smoothing:grayscale;
}}
a[x-apple-data-detectors] {{
color: inherit !important;
text-decoration: none !important;
}}
u + .emailify a {{
color: inherit !important;
text-decoration: none!important;
}}
#MessageViewBody a {{
color: inherit !important;
text-decoration: none!important;
}}
@media only screen and (max-width:1279px) {{
.emailify {{ height:100% !important; margin:0 !important; padding:0 !important; width:100% !important; }}
.m img {{ width: 100%!important; max-width: 100%!important; height: auto!important; }}
.u .m img {{ max-width: none!important; width: 100%!important; height: auto!important; }}
td.u {{ height:auto!important; }}
br.s {{ display: none!important; }}
td.b td {{ background-size: cover!important; }}
div.r > table > tbody > tr > td {{ direction: ltr!important; }}
img {{ background-color: transparent!important; }}
div.r.e > table > tbody > tr > td, div.r.e > div > table > tbody > tr > td {{ padding-right:16px!important }}
div.r.y > table > tbody > tr > td, div.r.y > div > table > tbody > tr > td {{ padding-left:16px!important }}
div.w.e > table > tbody > tr > td, div.w.e > div > table > tbody > tr > td {{ padding-right:16px!important; }}
div.w.y > table > tbody > tr > td, div.w.y > div > table > tbody > tr > td {{ padding-left:16px!important; }}
}}.tr-0 {{ }}
</style>
<meta name="format-detection" content="telephone=no, date=no, address=no, email=no, url=no">
<meta name="x-apple-disable-message-reformatting">
<meta name="color-scheme" content="light dark">
<meta name="supported-color-schemes" content="light dark">
<!--[if gte mso 9]>
<style>
a:link {{
mso-style-priority: 99;
color: inherit;
text-decoration: none;
}}
a:visited {{
mso-style-priority: 99;
color: inherit;
text-decoration: none;
}}
li {{ margin-left: -1em !important }}
table, td, p, div, span, ul, ol, li, a, h1, h2, h3, h4, h5, h6 {{
mso-hyphenate: none;
}}
sup, sub {{ font-size: 100% !important; }}
img {{ background-color: transparent !important; }}
</style>
<![endif]-->
<!--[if mso]>
<style>
.co {{ background: transparent !important; background-color: transparent !important; mso-padding-alt: 0px; !important; padding: 0px !important; border: 0px !important; border-top: 0px !important; border-right: 0px !important; border-bottom: 0px !important; border-left: 0px !important; }}
</style>
<![endif]-->
</head>
<body lang="en" link="#DD0000" vlink="#DD0000" class="emailify" style="mso-line-height-rule: exactly; mso-hyphenate: none; word-spacing: normal; background-color: #7e7e7e;">
<div style="background-color:#7e7e7e;" lang="en" dir="auto">
<!--[if mso | IE]><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1280px;" width="1280"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1280px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
<tbody>
<tr>
<td style="border-bottom:1px solid #bfbfbf;border-left:none;border-right:none;border-top:none;direction:ltr;font-size:0;padding:8px 16px 8px 16px;text-align:left;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:middle;width:1248px;"><![endif]-->
<div class="hn h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
<tbody>
<tr>
<td align="center" style="font-size:0;padding:0;word-break:break-word;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border-collapse:collapse;border-spacing:0;" class="m">
<tbody>
<tr>
<td style="width:344px;" class="m">
<img alt src="data:image/png;base64,{header_img}" style="border:0;display:block;outline:none;text-decoration:none;height:auto;width:100%;font-size:13px;" width="344">
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1280px;" width="1280" bgcolor="#fffffe"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div class="w e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1280px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
<tbody>
<tr>
<td style="border:none;direction:ltr;font-size:0;padding:16px 8px 16px 8px;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;border-radius:16px 16px 0px 0px;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;border-radius:16px 16px 0px 0px;">
<tbody>
<tr>
<td style="border:none;direction:ltr;font-size:0;padding:32px 24px 32px 24px;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:middle;width:600px;"><![endif]-->
<div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
<tbody>
<tr>
<td align="left" style="font-size:0;padding-bottom:24px;word-break:break-word;">
<div style="font-family:'Open Sans', Arial, sans-serif;font-size:16px;font-weight:400;line-height:150%;text-align:left;color:#2571e9;"><p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;"><span style="color:#090c02;">Hi,</span></p><p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;"><span style="color:#090c02;">&nbsp;</span></p><p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;"><span style="color:#090c02;">Your AODN data download request has been completed. Please click on the following link to access your data request.<br class="s"></span><span style="text-decoration:underline;">{object_url}</span></p></div>
</td>
</tr>
<tr>
<td align="center" class="b" style="font-size:0;padding:0;padding-bottom:0;word-break:break-word;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border-collapse:separate;width:244px;line-height:100%;">
<tbody>
<tr>
<td align="center" bgcolor="#3b6e8f" class="co" role="presentation" style="background:#3b6e8f;border:none;border-radius:4px 4px 4px 4px;cursor:auto;mso-padding-alt:12px 0px 12px 0px;vertical-align:middle;" valign="middle">
<!--[if mso]><v:roundrect style="width:244px;height:38px;v-text-anchor:middle;" arcsize="21%" fill="t" stroke="f" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:w="urn:schemas-microsoft-com:office:word"><w:anchorlock/><v:fill type="solid" color="#3b6e8f" /><v:textbox inset="0,0,0,0"><center><![endif]-->
<a href="{object_url}" class="co" style="display:inline-block;width:244px;background-color:#3b6e8f;color:#ffffff;font-family:'Open Sans', 'Arial', sans-serif;font-size:13px;font-weight:normal;line-height:100%;margin:0;text-decoration:none;text-transform:none;padding:12px 0px 12px 0px;mso-padding-alt:0;border-radius:4px 4px 4px 4px;" target="_blank">
<span style="display:inline;mso-hide:all;vertical-align:middle;"><img src="data:image/png;base64,{download_icon}" width="22" height="22" style="display:inline;mso-hide:all;vertical-align:middle;width:22px;height:22px;Margin-right:8px;"></span><span style="vertical-align:middle;font-size:16px;font-family:'Open Sans', 'Arial', sans-serif;font-weight:500;color:#ffffff;line-height:150%;mso-line-height-alt:24px;">Download</span>
</a>
<!--[if mso]></center></v:textbox></v:roundrect><![endif]-->
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div style="margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
<tbody>
<tr>
<td style="direction:ltr;font-size:0;padding:0;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:1264px;"><![endif]-->
<div class="o h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="font-size:0;padding:0;word-break:break-word;" aria-hidden="true">
<div style="height:0;line-height:0;">&#8202;</div>
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
<tbody>
<tr>
<td style="border:none;direction:ltr;font-size:0;padding:4px 24px 4px 24px;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:middle;width:600px;"><![endif]-->
<div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
<tbody>
<tr>
<td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
<table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
<tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="auto"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 16px; font-weight: 500; line-height: 150%; text-align: left; color: #090c02"><p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">Collection: IMOS - Ocean Gliders - delayed mode glider deployments</p></div></td></tr></table></td></tr>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div style="margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
<tbody>
<tr>
<td style="direction:ltr;font-size:0;padding:0;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:1264px;"><![endif]-->
<div class="o h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="font-size:0;padding:0;word-break:break-word;" aria-hidden="true">
<div style="height:0;line-height:0;">&#8202;</div>
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
<tbody>
<tr>
<td style="border:none;direction:ltr;font-size:0;padding:4px 24px 4px 24px;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:middle;width:600px;"><![endif]-->
<div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
<tbody>
<tr>
<td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
<table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
<tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="auto"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 150%; text-align: left; color: #090c02"><p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">Subsetting for this collection:</p></div></td></tr></table></td></tr>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div style="margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
<tbody>
<tr>
<td style="direction:ltr;font-size:0;padding:0;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:1264px;"><![endif]-->
<div class="o h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="font-size:0;padding:0;word-break:break-word;" aria-hidden="true">
<div style="height:0;line-height:0;">&#8202;</div>
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
<tbody>
<tr>
<td style="border:none;direction:ltr;font-size:0;padding:10px 24px 10px 24px;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:middle;width:600px;"><![endif]-->
<div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
<tbody>
<tr>
<td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
<table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
<tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="32"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"> <img alt width="32" style="display:block;width:32px;height:32px;" src="data:image/png;base64,{bbox_img}"></td></tr></table></td><td style="vertical-align:middle;color:transparent;font-size:0;" width="16">&#8203;</td><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="auto"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 500; line-height: 157%; text-align: left; color: #090c02"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">Bounding Box Selection</p></div></td></tr></table></td></tr>
</table>
</td>
</tr>
<tr>
<td style="font-size:0;padding:0;word-break:break-word;">
<div style="height:8px;line-height:8px;">&#8202;</div>
</td>
</tr>
<tr>
<td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0px 48px 0px 48px;word-break:break-word;">
<table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
<tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="500"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 400; line-height: 157%; text-align: left; color: #3c3c3c"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">N: -39.57346 </p></div></td></tr></table></td></tr><tr><td style="font-size:0;padding:0;padding-bottom:0;word-break:break-word;color:transparent;" aria-hidden="true"><div style="height:8px;line-height:8px;">&#8203;</div></td></tr><tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="500"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 400; line-height: 157%; text-align: left; color: #3c3c3c"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">S: -52.45856</p></div></td></tr></table></td></tr><tr><td style="font-size:0;padding:0;padding-bottom:0;word-break:break-word;color:transparent;" aria-hidden="true"><div style="height:8px;line-height:8px;">&#8203;</div></td></tr><tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="500"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 400; line-height: 157%; text-align: left; color: #3c3c3c"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">W: 65.45645</p></div></td></tr></table></td></tr><tr><td style="font-size:0;padding:0;padding-bottom:0;word-break:break-word;color:transparent;" aria-hidden="true"><div style="height:8px;line-height:8px;">&#8203;</div></td></tr><tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="500"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 400; line-height: 157%; text-align: left; color: #3c3c3c"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">E: 145.56345</p></div></td></tr></table></td></tr>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div style="margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
<tbody>
<tr>
<td style="direction:ltr;font-size:0;padding:0;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:1264px;"><![endif]-->
<div class="o h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="font-size:0;padding:0;word-break:break-word;" aria-hidden="true">
<div style="height:0;line-height:0;">&#8202;</div>
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
<tbody>
<tr>
<td style="border:none;direction:ltr;font-size:0;padding:10px 24px 10px 24px;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:middle;width:600px;"><![endif]-->
<div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
<tbody>
<tr>
<td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
<table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
<tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="32"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"> <img alt width="32" style="display:block;width:32px;height:32px;" src="data:image/png;base64,{time_range_img}"></td></tr></table></td><td style="vertical-align:middle;color:transparent;font-size:0;" width="16">&#8203;</td><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="auto"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 500; line-height: 157%; text-align: left; color: #090c02"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">Time Range</p></div></td></tr></table></td></tr>
</table>
</td>
</tr>
<tr>
<td style="font-size:0;padding:0;word-break:break-word;">
<div style="height:8px;line-height:8px;">&#8202;</div>
</td>
</tr>
<tr>
<td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0px 48px 0px 48px;word-break:break-word;">
<table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
<tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="500"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 500; line-height: 157%; text-align: left; color: #090c02"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">{conditions[0]} - {conditions[1]}</p></div></td></tr></table></td></tr>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div style="margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
<tbody>
<tr>
<td style="direction:ltr;font-size:0;padding:0;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:1264px;"><![endif]-->
<div class="o h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="font-size:0;padding:0;word-break:break-word;" aria-hidden="true">
<div style="height:0;line-height:0;">&#8202;</div>
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
<tbody>
<tr>
<td style="border:none;direction:ltr;font-size:0;padding:10px 24px 10px 24px;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:middle;width:600px;"><![endif]-->
<div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
<tbody>
<tr>
<td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
<table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
<tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="32"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"> <img alt width="32" style="display:block;width:32px;height:32px;" src="data:image/png;base64,{attributes_img}"></td></tr></table></td><td style="vertical-align:middle;color:transparent;font-size:0;" width="16">&#8203;</td><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="auto"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 500; line-height: 157%; text-align: left; color: #090c02"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">Attributes</p></div></td></tr></table></td></tr>
</table>
</td>
</tr>
<tr>
<td style="font-size:0;padding:0;word-break:break-word;">
<div style="height:8px;line-height:8px;">&#8202;</div>
</td>
</tr>
<tr>
<td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
<table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
<tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="auto"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 500; line-height: 157%; text-align: left; color: #090c02"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">Instrument: DALEC </p></div></td></tr></table></td></tr>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div style="margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
<tbody>
<tr>
<td style="direction:ltr;font-size:0;padding:0;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:1264px;"><![endif]-->
<div class="o h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="font-size:0;padding:0;word-break:break-word;" aria-hidden="true">
<div style="height:0;line-height:0;">&#8202;</div>
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
<tbody>
<tr>
<td style="border:none;direction:ltr;font-size:0;padding:4px 24px 4px 24px;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:middle;width:600px;"><![endif]-->
<div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
<tbody>
<tr>
<td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
<table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
<tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="600"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 16px; font-weight: 400; line-height: 150%; text-align: left; color: #090c02"><p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">uuid is: {uuid}</p></div></td></tr></table></td></tr>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div style="margin:0px auto;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
<tbody>
<tr>
<td style="direction:ltr;font-size:0;padding:0;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:1264px;"><![endif]-->
<div class="o h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="font-size:0;padding:0;word-break:break-word;" aria-hidden="true">
<div style="height:0;line-height:0;">&#8202;</div>
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;border-radius:0px 0px 16px 16px;max-width:1264px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;border-radius:0px 0px 16px 16px;">
<tbody>
<tr>
<td style="border:none;direction:ltr;font-size:0;padding:40px 24px 40px 24px;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:middle;width:600px;"><![endif]-->
<div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
<tbody>
<tr>
<td align="left" style="font-size:0;word-break:break-word;">
<div style="font-family:'Open Sans', Arial, sans-serif;font-size:16px;font-weight:400;line-height:150%;text-align:left;color:#090c02;"><p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">The download will be available for 7 days.</p><p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">If you require assistance with this service please contact us with <span style="color:#2571e9;">info@aodn.org.au</span>. </p><p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">&nbsp;</p><p style="Margin:0;mso-line-height-alt:23px;font-size:16px;line-height:153%;"><span style="font-size:15px;color:#000000;line-height:153%;mso-line-height-alt:23px;">Kind regards,</span></p><p style="Margin:0;mso-line-height-alt:23px;font-size:16px;line-height:153%;"><span style="font-size:15px;color:#000000;line-height:153%;mso-line-height-alt:23px;">Australian Ocean Data Network</span></p></div>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td><td style="vertical-align:top;width:308px;"><![endif]-->
<div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
<tr>
<td style="vertical-align:top;padding:0;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
<tbody>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1280px;" width="1280"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
<div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1280px;">
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
<tbody>
<tr>
<td style="border-bottom:none;border-left:none;border-right:none;border-top:1px solid #bfbfbf;direction:ltr;font-size:0;padding:32px 32px 60px 32px;text-align:center;">
<!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:middle;width:1216px;"><![endif]-->
<div class="ci h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
<tbody>
<tr>
<td align="center" style="font-size:0;padding:0;padding-bottom:16px;word-break:break-word;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border-collapse:collapse;border-spacing:0;" class="m">
<tbody>
<tr>
<td style="width:600px;" class="m">
<img alt src="data:image/png;base64,{footer_text_img}" style="border:0;display:block;outline:none;text-decoration:none;height:auto;width:100%;font-size:13px;" width="600">
</td>
</tr>
</tbody>
</table>
</td>
</tr>
<tr>
<td align="center" style="font-size:0;padding:0;padding-bottom:16px;word-break:break-word;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border-collapse:collapse;border-spacing:0;" class="m">
<tbody>
<tr>
<td style="width:600px;" class="m">
<img alt src="data:image/png;base64,{footer_action_img}" style="border:0;display:block;outline:none;text-decoration:none;height:auto;width:100%;font-size:13px;" width="600">
</td>
</tr>
</tbody>
</table>
</td>
</tr>
<tr>
<td align="center" style="font-size:0;padding:0;padding-bottom:0;word-break:break-word;">
<!--[if mso | IE]><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation"><tr><td><![endif]-->
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="float:none;display:inline-table;">
<tbody>
<tr>
<td style="padding:0 16px 0 0;vertical-align:middle;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:24px;">
<tbody>
<tr>
<td style="font-size:0;height:24px;vertical-align:middle;width:24px;">
<a href="#insertUrlLink" target="_blank">
<img alt="Facebook" height="24" src="data:image/png;base64,{facebook_img}" style="display:block;" width="24">
</a>
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
<!--[if mso | IE]></td><td><![endif]-->
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="float:none;display:inline-table;">
<tbody>
<tr>
<td style="padding:0 16px 0 0;vertical-align:middle;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:24px;">
<tbody>
<tr>
<td style="font-size:0;height:24px;vertical-align:middle;width:24px;">
<a href="#insertUrlLink" target="_blank">
<img alt="Instagram" height="24" src="data:image/png;base64,{instagram_img}" style="display:block;" width="24">
</a>
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
<!--[if mso | IE]></td><td><![endif]-->
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="float:none;display:inline-table;">
<tbody>
<tr>
<td style="padding:0 16px 0 0;vertical-align:middle;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:24px;">
<tbody>
<tr>
<td style="font-size:0;height:24px;vertical-align:middle;width:24px;">
<a href="#insertUrlLink" target="_blank">
<img alt="X" height="24" src="data:image/png;base64,{butterfly_img}" style="display:block;" width="24">
</a>
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
<!--[if mso | IE]></td><td><![endif]-->
<table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="float:none;display:inline-table;">
<tbody>
<tr>
<td style="padding:0;padding-right:0;vertical-align:middle;">
<table border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:24px;">
<tbody>
<tr>
<td style="font-size:0;height:24px;vertical-align:middle;width:24px;">
<a href="#insertUrlLink" target="_blank">
<img alt="YouTube" height="24" src="data:image/png;base64,{linkedin_img}" style="display:block;" width="24">
</a>
</td>
</tr>
</tbody>
</table>
</td>
</tr>
</tbody>
</table>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</td>
</tr>
</tbody>
</table>
</div>
<!--[if mso | IE]></td></tr></table><![endif]-->
</div>
</body>
</html>
"""