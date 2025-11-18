from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.email_templates.download_button import (
    generate_download_button_html,
)
from data_access_service.utils.email_templates.form_subsetting_divs import (
    form_subsetting_divs,
)
from data_access_service.utils.email_templates.email_images import (
    HEADER_IMG,
    FACEBOOK_IMG,
    INSTAGRAM_IMG,
    BLUESKY_IMG,
    LINKEDIN_IMG,
    CONTACT_IMG,
)

# footer links
FACEBOOK_URL = "https://www.facebook.com/IntegratedMarineObservingSystem"
LINKEDIN_URL = "https://www.linkedin.com/company/imos_aus/posts/?feedView=all"
X_URL = "https://x.com/IMOS_AUS"
BLUESKY_URL = "https://bsky.app/profile/imos-aus.bsky.social"
INSTAGRAM_URL = "https://www.instagram.com/imos_australia/#"
CONTACT_US_URL = "mailto:info@aodn.org.au"
TERMS_OF_USE_URL = "https://imos.org.au/terms-of-use"
ACKNOWLEDGE_US_URL = "https://imos.org.au/resources/acknowledging-us"
CONDITIONS_OF_USE_URL = "https://imos.org.au/conditions-of-use"


def get_download_email_html_body(
    subset_request: SubsetRequest, object_urls: [str]
) -> str:

    if not object_urls or len(object_urls) == 0:
        return "<p>No data found for your selected subset.</p>"

    start_date = subset_request.start_date
    end_date = subset_request.end_date
    bboxes = subset_request.bboxes if subset_request.bboxes else []
    subsetting_section = form_subsetting_divs(start_date, end_date, bboxes)
    object_url_str = "<br>".join(
        [
            f'<a href="{url}" style="color:#2571e9;text-decoration:underline;">{url}</a>'
            for url in object_urls
        ]
    )
    collection_title = subset_request.collection_title
    full_metadata_link = subset_request.full_metadata_link
    suggested_citation = subset_request.suggested_citation

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
         .d {{ width:304px !important; max-width: 304px; }}
         .g6 {{ width:640px !important; max-width: 640px; }}
         .c {{ width:600px !important; max-width: 600px; }}
         .o {{ width:100% !important; max-width: 100%; }}
         .oe {{ width:288px !important; max-width: 288px; }}
         .h7 {{ width:326px !important; max-width: 326px; }}
         .j {{ width:148px !important; max-width: 148px; }}
         .hr {{ width:16px !important; max-width: 16px; }}
         .a {{ width:112px !important; max-width: 112px; }}
         .d3w {{ width:142px !important; max-width: 142px; }}
         .g6q {{ width:145px !important; max-width: 145px; }}
         .i {{ width:327px !important; max-width: 327px; }}
         .v {{ width:24.359% !important; max-width: 24.359%; }}
         .q {{ width:51.282% !important; max-width: 51.282%; }}
         }}
      </style>
      <style media="screen and (min-width:1279px)">
         .moz-text-html .d {{ width:304px !important; max-width: 304px; }}
         .moz-text-html .g6 {{ width:640px !important; max-width: 640px; }}
         .moz-text-html .c {{ width:600px !important; max-width: 600px; }}
         .moz-text-html .o {{ width:100% !important; max-width: 100%; }}
         .moz-text-html .oe {{ width:288px !important; max-width: 288px; }}
         .moz-text-html .h7 {{ width:326px !important; max-width: 326px; }}
         .moz-text-html .j {{ width:148px !important; max-width: 148px; }}
         .moz-text-html .hr {{ width:16px !important; max-width: 16px; }}
         .moz-text-html .a {{ width:112px !important; max-width: 112px; }}
         .moz-text-html .d3w {{ width:142px !important; max-width: 142px; }}
         .moz-text-html .g6q {{ width:145px !important; max-width: 145px; }}
         .moz-text-html .i {{ width:327px !important; max-width: 327px; }}
         .moz-text-html .v {{ width:24.359% !important; max-width: 24.359%; }}
         .moz-text-html .q {{ width:51.282% !important; max-width: 51.282%; }}
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
         }}div.wa {{ font-size: 0; }}
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
         td.b.x > table {{ width:100%!important }} td.x > table > tbody > tr > td > a {{ display: block!important; width: 100%!important; padding-left: 0!important; padding-right: 0!important; }}
         td.b.x > table {{ width:100%!important }} td.x > table > tbody > tr > td {{ width: 100%!important; padding-left: 0!important; padding-right: 0!important; }}
         div.g.l > table > tbody > tr > td {{ padding-bottom:16px!important }}
         }}.tr-0 {{}}
      </style>
      <meta name="format-detection" content="telephone=no, date=no, address=no, email=no, url=no">
      <meta name="x-apple-disable-message-reformatting">
      <meta name="color-scheme" content="light dark">
      <meta name="supported-color-schemes" content="light dark">
      <!--[if gte mso 9]>
      <style>
         a:link {{
         mso-style-priority: 99;
         color:#2571e9 important;
         text-decoration: none;
         }}
         a:visited {{
         mso-style-priority: 99;
         color:#2571e9 important;
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
         <!--[if mso | IE]>
         <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1280px;" width="1280">
            <tr>
               <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                  <![endif]-->
                  <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1280px;">
                     <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                        <tbody>
                           <tr>
                              <td style="border-bottom:1px solid #bfbfbf;border-left:none;border-right:none;border-top:none;direction:ltr;font-size:0;padding:24px 16px 24px 16px;text-align:center;">
                                 <!--[if mso | IE]>
                                 <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                    <tr>
                                       <td style="vertical-align:top;width:304px;">
                                          <![endif]-->
                                          <div class="d h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td style="vertical-align:top;padding:0;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                            <tbody></tbody>
                                                         </table>
                                                      </td>
                                                   </tr>
                                                </tbody>
                                             </table>
                                          </div>
                                          <!--[if mso | IE]>
                                       </td>
                                       <td style="vertical-align:middle;width:640px;">
                                          <![endif]-->
                                          <div class="g6 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td align="center" class="x" style="font-size:0;padding:0;word-break:break-word;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border-collapse:collapse;border-spacing:0;" class="m">
                                                            <tbody>
                                                               <tr>
                                                                  <td style="width:344px;" class="m">
                                                                     <img alt src="data:image/png;base64,{HEADER_IMG}" style="border:0;display:block;outline:none;text-decoration:none;height:auto;width:100%;font-size:13px;" width="344">
                                                                  </td>
                                                               </tr>
                                                            </tbody>
                                                         </table>
                                                      </td>
                                                   </tr>
                                                </tbody>
                                             </table>
                                          </div>
                                          <!--[if mso | IE]>
                                       </td>
                                       <td style="vertical-align:top;width:304px;">
                                          <![endif]-->
                                          <div class="d h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td style="vertical-align:top;padding:0;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                            <tbody></tbody>
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
                  <!--[if mso | IE]>
               </td>
            </tr>
         </table>
         <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1280px;" width="1280" bgcolor="#fffffe">
            <tr>
               <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                  <![endif]-->
                  <div class="w e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1280px;">
                     <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                        <tbody>
                           <tr>
                              <td style="border:none;direction:ltr;font-size:0;padding:16px 16px 16px 16px;text-align:center;">
                                 <!--[if mso | IE]>
                                 <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;border-radius:16px 16px 0px 0px;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;border-radius:16px 16px 0px 0px;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="border:none;direction:ltr;font-size:0;padding:24px 20px 24px 20px;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:middle;width:600px;">
                                                                           <![endif]-->
                                                                           <div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                                                                              <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                                                                                 <tbody>
                                                                                    <tr>
                                                                                       <td align="left" style="font-size:0;padding-bottom:24px;word-break:break-word;">
                                                                                          <div style="font-family:'Open Sans', 'Arial', sans-serif;font-size:16px;font-weight:400;line-height:150%;text-align:left;color:#2571e9;">
                                                                                             <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;"><span style="color:#090c02;">Hi,</span></p>
                                                                                             <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;"><span style="color:#090c02;">&nbsp;</span></p>
                                                                                             <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;"><span style="color:#090c02;">Your AODN data download request has been completed. Please click on the following link to access your data request.<br class="s"></span><span style="text-decoration:underline;">{object_url_str}</span></p>
                                                                                          </div>
                                                                                       </td>
                                                                                    </tr>
                                                                                    {generate_download_button_html(object_urls)}
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div style="margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="direction:ltr;font-size:0;padding:0;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:top;width:1248px;">
                                                                           <![endif]-->
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="border:none;direction:ltr;font-size:0;padding:4px 20px 4px 20px;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:middle;width:600px;">
                                                                           <![endif]-->
                                                                           <div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
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
                                                                                                               <p style="Margin:0;mso-line-height-alt:24px;font-size:17px;line-height:141%;">Collection</p>
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div style="margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="direction:ltr;font-size:0;padding:0;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:top;width:1248px;">
                                                                           <![endif]-->
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="border:none;direction:ltr;font-size:0;padding:4px 20px 4px 20px;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:middle;width:600px;">
                                                                           <![endif]-->
                                                                           <div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
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
                                                                                                            <div style="font-family: 'Open Sans', 'Arial', sans-serif; font-size: 16px; font-weight: 400; line-height: 150%; text-align: left; color: #090c02">
                                                                                                               <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">{collection_title}</p>
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div style="margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="direction:ltr;font-size:0;padding:0;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:top;width:1248px;">
                                                                           <![endif]-->
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    {subsetting_section}
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="border:none;direction:ltr;font-size:0;padding:4px 20px 4px 20px;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:middle;width:600px;">
                                                                           <![endif]-->
                                                                           <div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                                                                              <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                                                                                 <tbody>
                                                                                    <tr>
                                                                                       <td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
                                                                                          <table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
                                                                                             <tr>
                                                                                                <td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="600">
                                                                                                   <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                                                                                      <tr>
                                                                                                         <td align="left" width="100%">
                                                                                                            <div style="font-family: 'Open Sans', 'Arial', sans-serif; font-size: 16px; font-weight: 400; line-height: 150%; text-align: left; color: #090c02">
                                                                                                               <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">Metadata Link: <a href="{full_metadata_link}" style="color:#2571e9;text-decoration:underline;">{full_metadata_link}</a></p>
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div style="margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="direction:ltr;font-size:0;padding:0;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:top;width:1248px;">
                                                                           <![endif]-->
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="border:none;direction:ltr;font-size:0;padding:16px 20px 4px 20px;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:middle;width:600px;">
                                                                           <![endif]-->
                                                                           <div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
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
                                                                                                               <p style="Margin:0;mso-line-height-alt:24px;font-size:17px;line-height:141%;">Suggested Citation</p>
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div style="margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="direction:ltr;font-size:0;padding:0;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:top;width:1248px;">
                                                                           <![endif]-->
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="border:none;direction:ltr;font-size:0;padding:4px 20px 4px 20px;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:middle;width:600px;">
                                                                           <![endif]-->
                                                                           <div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
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
                                                                                                            <div style="font-family: 'Open Sans', 'Arial', sans-serif; font-size: 16px; font-weight: 400; line-height: 150%; text-align: left; color: #090c02">
                                                                                                               <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">{suggested_citation}</p>
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div style="margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="direction:ltr;font-size:0;padding:0;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:top;width:1248px;">
                                                                           <![endif]-->
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="border:none;direction:ltr;font-size:0;padding:16px 20px 4px 20px;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:middle;width:600px;">
                                                                           <![endif]-->
                                                                           <div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
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
                                                                                                               <p style="Margin:0;mso-line-height-alt:24px;font-size:17px;line-height:141%;">Usage Constrains</p>
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div style="margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="direction:ltr;font-size:0;padding:0;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:top;width:1248px;">
                                                                           <![endif]-->
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="border:none;direction:ltr;font-size:0;padding:4px 20px 4px 20px;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:middle;width:600px;">
                                                                           <![endif]-->
                                                                           <div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
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
                                                                                                            <div style="font-family: 'Open Sans', 'Arial', sans-serif; font-size: 16px; font-weight: 400; line-height: 150%; text-align: left; color: #090c02">
                                                                                                               <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">Any users of IMOS data are required to clearly acknowledge the source of the material derived from IMOS in the format: "Data was sourced from Australia's Integrated Marine Observing System (IMOS) - IMOS is enabled by the National Collaborative Research Infrastructure strategy (NCRIS)." If relevant, also credit other organisations involved in collection of this particular datastream (as listed in 'credit' in the metadata record).</p>
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div style="margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="direction:ltr;font-size:0;padding:0;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:top;width:1248px;">
                                                                           <![endif]-->
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="border:none;direction:ltr;font-size:0;padding:4px 20px 4px 20px;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:middle;width:600px;">
                                                                           <![endif]-->
                                                                           <div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                                                                              <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                                                                                 <tbody>
                                                                                    <tr>
                                                                                       <td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
                                                                                          <table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
                                                                                             <tr>
                                                                                                <td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="600">
                                                                                                   <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                                                                                      <tr>
                                                                                                         <td align="left" width="100%">
                                                                                                            <div style="font-family: 'Open Sans', 'Arial', sans-serif; font-size: 16px; font-weight: 400; line-height: 150%; text-align: left; color: #090c02">
                                                                                                               <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">If using data from the Ningaloo (TAN100) mooring, please add to the citation - "Department of Jobs, Tourism, Science and Innovation (DJTSI), Western Australian Government". If using data from the Ocean Reference Station 65m (ORS065) mooring, please add to the citation - "Sydney Water Corporation".</p>
                                                                                                               <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">Data, products and services from IMOS are provided "as is" without any warranty as to fitness for a particular purpose. By using this data you are accepting the license agreement and terms specified above. You accept all risks and responsibility for losses, damages, costs and other consequences resulting directly or indirectly from using this site and any information or material available from it.</p>
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div style="margin:0px auto;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="direction:ltr;font-size:0;padding:0;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:top;width:1248px;">
                                                                           <![endif]-->
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
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                    <tr>
                                       <td width="1280px">
                                          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1248px;" width="1248">
                                             <tr>
                                                <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                                                   <![endif]-->
                                                   <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;border-radius:0px 0px 16px 16px;max-width:1248px;">
                                                      <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;border-radius:0px 0px 16px 16px;">
                                                         <tbody>
                                                            <tr>
                                                               <td style="border:none;direction:ltr;font-size:0;padding:24px 20px 40px 20px;text-align:center;">
                                                                  <!--[if mso | IE]>
                                                                  <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                                                     <tr>
                                                                        <td style="vertical-align:middle;width:600px;">
                                                                           <![endif]-->
                                                                           <div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                                                                              <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                                                                                 <tbody>
                                                                                    <tr>
                                                                                       <td align="left" style="font-size:0;word-break:break-word;">
                                                                                          <div style="font-family:'Open Sans', 'Arial', sans-serif;font-size:16px;font-weight:400;line-height:150%;text-align:left;color:#090c02;">
                                                                                             <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">The download will be available for 7 days.</p>
                                                                                             <p style="Margin:0;mso-line-height-alt:24px;font-size:16px;line-height:150%;">If you require assistance with this service please contact us with <span style="color:#2571e9;"><a href="mailto:info@aodn.org.au" style="color:#2571e9;text-decoration:underline;">info@aodn.org.au</a></span>. </p>
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
                                                   </div>
                                                   <!--[if mso | IE]>
                                                </td>
                                             </tr>
                                          </table>
                                       </td>
                                    </tr>
                                 </table>
                                 <![endif]-->
                              </td>
                           </tr>
                        </tbody>
                     </table>
                  </div>
                  <!--[if mso | IE]>
               </td>
            </tr>
         </table>
         <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1280px;" width="1280">
            <tr>
               <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                  <![endif]-->
                  <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1280px;">
                     <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                        <tbody>
                           <tr>
                              <td style="border-bottom:none;border-left:none;border-right:none;border-top:1px solid #bfbfbf;direction:ltr;font-size:0;padding:24px 32px 8px 32px;text-align:center;">
                                 <!--[if mso | IE]>
                                 <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                    <tr>
                                       <td style="vertical-align:top;width:288px;">
                                          <![endif]-->
                                          <div class="oe h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td style="vertical-align:top;padding:0;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                            <tbody></tbody>
                                                         </table>
                                                      </td>
                                                   </tr>
                                                </tbody>
                                             </table>
                                          </div>
                                          <!--[if mso | IE]>
                                       </td>
                                       <td style="vertical-align:middle;width:640px;">
                                          <![endif]-->
                                          <div class="g6 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td style="border:none;vertical-align:middle;padding:0px 20px 0px 20px;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                            <tbody>
                                                               <tr>
                                                                  <td align="left" style="font-size:0;padding-bottom:8px;word-break:break-word;">
                                                                     <div style="font-family:'Open Sans', 'Arial', sans-serif;font-size:14px;font-weight:400;line-height:157%;text-align:left;color:#3c3c3c;">
                                                                        <p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">The Australian Ocean Data Network (AODN) stands at the forefront of marine data management in Australia, providing an essential infrastructure for the discovery, sharing and reuse of comprehensive marine and climate data. Our commitment to making these data freely available underscores our dedication to fostering an informed and engaged public, promoting sustainable environmental practices and driving economic growth through innovation.</p>
                                                                     </div>
                                                                  </td>
                                                               </tr>
                                                               <tr>
                                                                  <td style="font-size:0;padding:0;padding-bottom:0;word-break:break-word;" aria-hidden="true">
                                                                     <div style="height:4px;line-height:4px;">&#8202;</div>
                                                                  </td>
                                                               </tr>
                                                            </tbody>
                                                         </table>
                                                      </td>
                                                   </tr>
                                                </tbody>
                                             </table>
                                          </div>
                                          <!--[if mso | IE]>
                                       </td>
                                       <td style="vertical-align:top;width:288px;">
                                          <![endif]-->
                                          <div class="oe h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td style="vertical-align:top;padding:0;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                            <tbody></tbody>
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
                  <!--[if mso | IE]>
               </td>
            </tr>
         </table>
         <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1280px;" width="1280">
            <tr>
               <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                  <![endif]-->
                  <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1280px;">
                     <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                        <tbody>
                           <tr>
                              <td style="border:none;direction:ltr;font-size:0;padding:4px 16px 4px 16px;text-align:center;">
                                 <!--[if mso | IE]>
                                 <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                    <tr>
                                       <td style="vertical-align:top;width:326px;">
                                          <![endif]-->
                                          <div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td style="vertical-align:top;padding:0;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                            <tbody></tbody>
                                                         </table>
                                                      </td>
                                                   </tr>
                                                </tbody>
                                             </table>
                                          </div>
                                          <!--[if mso | IE]>
                                       </td>
                                       <td style="vertical-align:middle;width:148px;">
                                          <![endif]-->
                                          <div class="j h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td align="left" class="b x" style="font-size:0;padding:0;word-break:break-word;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border-collapse:separate;width:148px;line-height:100%;">
                                                            <tbody>
                                                               <tr>
                                                                  <td align="center" bgcolor="#595959" class="co" role="presentation" style="background:#595959;border:none;border-radius:8px 8px 8px 8px;cursor:auto;mso-padding-alt:8px 0px 8px 0px;vertical-align:middle;" valign="middle">
                                                                     <!--[if mso]>
                                                                     <v:roundrect style="width:148px;height:39px;v-text-anchor:middle;" arcsize="41%" fill="t" stroke="f" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:w="urn:schemas-microsoft-com:office:word">
                                                                        <w:anchorlock/>
                                                                        <v:fill type="solid" color="#595959" />
                                                                        <v:textbox inset="0,0,0,0">
                                                                           <center>
                                                                              <![endif]-->
                                                                              <a href="{CONTACT_US_URL}" class="co" style="display:inline-block;width:148px;background-color:#595959;color:#ffffff;font-family:'Open Sans', 'Arial', sans-serif;font-size:13px;font-weight:normal;line-height:100%;margin:0;text-decoration:none;text-transform:none;padding:8px 0px 8px 0px;mso-padding-alt:0;border-radius:8px 8px 8px 8px;" target="_blank">
                                                                              <span style="display:inline;mso-hide:all;vertical-align:middle;"><img src="data:image/png;base64,{CONTACT_IMG}" width="23" height="23" style="display:inline;mso-hide:all;vertical-align:middle;width:23px;height:23px;Margin-right:8px;"></span><span style="vertical-align:middle;font-size:14px;font-family:'Open Sans', 'Arial', sans-serif;font-weight:500;color:#ffffff;line-height:157%;mso-line-height-alt:22px;">Contact Us</span>
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
                                                </tbody>
                                             </table>
                                          </div>
                                          <!--[if mso | IE]>
                                       </td>
                                       <td style="vertical-align:top;width:16px;">
                                          <![endif]-->
                                          <div class="hr h g l" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td style="vertical-align:top;padding:0;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                            <tbody></tbody>
                                                         </table>
                                                      </td>
                                                   </tr>
                                                </tbody>
                                             </table>
                                          </div>
                                          <!--[if mso | IE]>
                                       </td>
                                       <td style="vertical-align:middle;width:112px;">
                                          <![endif]-->
                                          <div class="a h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td align="left" class="b x" style="font-size:0;padding:0;word-break:break-word;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border-collapse:separate;width:112px;line-height:100%;">
                                                            <tbody>
                                                               <tr>
                                                                  <td align="center" bgcolor="transparent" role="presentation" style="background:transparent;border:none;border-radius:8px 8px 8px 8px;cursor:auto;mso-padding-alt:12px 0px 12px 0px;vertical-align:middle;" valign="middle">
                                                                     <a href="{TERMS_OF_USE_URL}" style="display:inline-block;width:112px;color:#ffffff;font-family:'Open Sans', 'Arial', sans-serif;font-size:13px;font-weight:normal;line-height:100%;margin:0;text-decoration:none;text-transform:none;padding:12px 0px 12px 0px;mso-padding-alt:0;border-radius:8px 8px 8px 8px;" target="_blank">
                                                                     <span style="font-size:14px;font-family:'Open Sans', 'Arial', sans-serif;font-weight:500;color:#000000;line-height:157%;mso-line-height-alt:22px;">Terms of Use</span>
                                                                     </a>
                                                                  </td>
                                                               </tr>
                                                            </tbody>
                                                         </table>
                                                      </td>
                                                   </tr>
                                                </tbody>
                                             </table>
                                          </div>
                                          <!--[if mso | IE]>
                                       </td>
                                       <td style="vertical-align:top;width:16px;">
                                          <![endif]-->
                                          <div class="hr h g l" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td style="vertical-align:top;padding:0;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                            <tbody></tbody>
                                                         </table>
                                                      </td>
                                                   </tr>
                                                </tbody>
                                             </table>
                                          </div>
                                          <!--[if mso | IE]>
                                       </td>
                                       <td style="vertical-align:middle;width:142px;">
                                          <![endif]-->
                                          <div class="d3w h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td align="left" class="b x" style="font-size:0;padding:0;word-break:break-word;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border-collapse:separate;width:142px;line-height:100%;">
                                                            <tbody>
                                                               <tr>
                                                                  <td align="center" bgcolor="transparent" role="presentation" style="background:transparent;border:none;border-radius:8px 8px 8px 8px;cursor:auto;mso-padding-alt:12px 0px 12px 0px;vertical-align:middle;" valign="middle">
                                                                     <a href="{CONDITIONS_OF_USE_URL}" style="display:inline-block;width:142px;color:#ffffff;font-family:'Open Sans', 'Arial', sans-serif;font-size:13px;font-weight:normal;line-height:100%;margin:0;text-decoration:none;text-transform:none;padding:12px 0px 12px 0px;mso-padding-alt:0;border-radius:8px 8px 8px 8px;" target="_blank">
                                                                     <span style="font-size:14px;font-family:'Open Sans', 'Arial', sans-serif;font-weight:500;color:#000000;line-height:157%;mso-line-height-alt:22px;">Conditions of Use</span>
                                                                     </a>
                                                                  </td>
                                                               </tr>
                                                            </tbody>
                                                         </table>
                                                      </td>
                                                   </tr>
                                                </tbody>
                                             </table>
                                          </div>
                                          <!--[if mso | IE]>
                                       </td>
                                       <td style="vertical-align:top;width:16px;">
                                          <![endif]-->
                                          <div class="hr h g l" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td style="vertical-align:top;padding:0;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                            <tbody></tbody>
                                                         </table>
                                                      </td>
                                                   </tr>
                                                </tbody>
                                             </table>
                                          </div>
                                          <!--[if mso | IE]>
                                       </td>
                                       <td style="vertical-align:middle;width:145px;">
                                          <![endif]-->
                                          <div class="g6q h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td align="left" class="b x" style="font-size:0;padding:0;word-break:break-word;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border-collapse:separate;width:145px;line-height:100%;">
                                                            <tbody>
                                                               <tr>
                                                                  <td align="center" bgcolor="transparent" role="presentation" style="background:transparent;border:none;border-radius:8px 8px 8px 8px;cursor:auto;mso-padding-alt:12px 0px 12px 0px;vertical-align:middle;" valign="middle">
                                                                     <a href="{ACKNOWLEDGE_US_URL}" style="display:inline-block;width:145px;color:#ffffff;font-family:'Open Sans', 'Arial', sans-serif;font-size:13px;font-weight:normal;line-height:100%;margin:0;text-decoration:none;text-transform:none;padding:12px 0px 12px 0px;mso-padding-alt:0;border-radius:8px 8px 8px 8px;" target="_blank">
                                                                     <span style="font-size:14px;font-family:'Open Sans', 'Arial', sans-serif;font-weight:500;color:#000000;line-height:157%;mso-line-height-alt:22px;">Acknowledging Us</span>
                                                                     </a>
                                                                  </td>
                                                               </tr>
                                                            </tbody>
                                                         </table>
                                                      </td>
                                                   </tr>
                                                </tbody>
                                             </table>
                                          </div>
                                          <!--[if mso | IE]>
                                       </td>
                                       <td style="vertical-align:top;width:327px;">
                                          <![endif]-->
                                          <div class="i h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                <tbody>
                                                   <tr>
                                                      <td style="vertical-align:top;padding:0;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                            <tbody></tbody>
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
                  <!--[if mso | IE]>
               </td>
            </tr>
         </table>
         <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1280px;" width="1280">
            <tr>
               <td style="line-height:0;font-size:0;mso-line-height-rule:exactly;">
                  <![endif]-->
                  <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1280px;">
                     <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
                        <tbody>
                           <tr>
                              <td style="border:none;direction:ltr;font-size:0;padding:12px 16px 32px 16px;text-align:center;">
                                 <!--[if mso | IE]>
                                 <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                                    <tr>
                                       <td style="width:1248px;">
                                          <![endif]-->
                                          <div class="o h wa" style="font-size:0;line-height:0;text-align:left;display:inline-block;width:100%;direction:ltr;">
                                             <!--[if mso | IE]>
                                             <table border="0" cellpadding="0" cellspacing="0" role="presentation">
                                                <tr>
                                                   <td style="vertical-align:top;width:304px;">
                                                      <![endif]-->
                                                      <div class="v h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:24.359%;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                            <tbody>
                                                               <tr>
                                                                  <td style="vertical-align:top;padding:0;">
                                                                     <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                                        <tbody></tbody>
                                                                     </table>
                                                                  </td>
                                                               </tr>
                                                            </tbody>
                                                         </table>
                                                      </div>
                                                      <!--[if mso | IE]>
                                                   </td>
                                                   <td style="vertical-align:middle;width:639px;">
                                                      <![endif]-->
                                                      <div class="q h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:51.282%;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                                                            <tbody>
                                                               <tr>
                                                                  <td align="center" style="font-size:0;padding:0;word-break:break-word;">
                                                                     <!--[if mso | IE]>
                                                                     <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation">
                                                                        <tr>
                                                                           <td>
                                                                              <![endif]-->
                                                                              <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="float:none;display:inline-table;">
                                                                                 <tbody>
                                                                                    <tr>
                                                                                       <td style="padding:0 16px 0 0;vertical-align:middle;">
                                                                                          <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:24px;">
                                                                                             <tbody>
                                                                                                <tr>
                                                                                                   <td style="font-size:0;height:24px;vertical-align:middle;width:24px;">
                                                                                                      <a href="{INSTAGRAM_URL}" target="_blank">
                                                                                                      <img alt="Instagram" height="24" src="data:image/png;base64,{INSTAGRAM_IMG}" style="display:block;" width="24">
                                                                                                      </a>
                                                                                                   </td>
                                                                                                </tr>
                                                                                             </tbody>
                                                                                          </table>
                                                                                       </td>
                                                                                    </tr>
                                                                                 </tbody>
                                                                              </table>
                                                                              <!--[if mso | IE]>
                                                                           </td>
                                                                           <td>
                                                                              <![endif]-->
                                                                              <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="float:none;display:inline-table;">
                                                                                 <tbody>
                                                                                    <tr>
                                                                                       <td style="padding:0 16px 0 0;vertical-align:middle;">
                                                                                          <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:24px;">
                                                                                             <tbody>
                                                                                                <tr>
                                                                                                   <td style="font-size:0;height:24px;vertical-align:middle;width:24px;">
                                                                                                      <a href="{FACEBOOK_URL}" target="_blank">
                                                                                                      <img alt="Facebook" height="24" src="data:image/png;base64,{FACEBOOK_IMG}" style="display:block;" width="24">
                                                                                                      </a>
                                                                                                   </td>
                                                                                                </tr>
                                                                                             </tbody>
                                                                                          </table>
                                                                                       </td>
                                                                                    </tr>
                                                                                 </tbody>
                                                                              </table>
                                                                              <!--[if mso | IE]>
                                                                           </td>
                                                                           <td>
                                                                              <![endif]-->
                                                                              <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="float:none;display:inline-table;">
                                                                                 <tbody>
                                                                                    <tr>
                                                                                       <td style="padding:0 16px 0 0;vertical-align:middle;">
                                                                                          <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:24px;">
                                                                                             <tbody>
                                                                                                <tr>
                                                                                                   <td style="font-size:0;height:24px;vertical-align:middle;width:24px;">
                                                                                                      <a href="{BLUESKY_URL}" target="_blank">
                                                                                                      <img alt="X" height="24" src="data:image/png;base64,{BLUESKY_IMG}" style="display:block;" width="24">
                                                                                                      </a>
                                                                                                   </td>
                                                                                                </tr>
                                                                                             </tbody>
                                                                                          </table>
                                                                                       </td>
                                                                                    </tr>
                                                                                 </tbody>
                                                                              </table>
                                                                              <!--[if mso | IE]>
                                                                           </td>
                                                                           <td>
                                                                              <![endif]-->
                                                                              <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="float:none;display:inline-table;">
                                                                                 <tbody>
                                                                                    <tr>
                                                                                       <td style="padding:0;padding-right:0;vertical-align:middle;">
                                                                                          <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:24px;">
                                                                                             <tbody>
                                                                                                <tr>
                                                                                                   <td style="font-size:0;height:24px;vertical-align:middle;width:24px;">
                                                                                                      <a href="{LINKEDIN_URL}" target="_blank">
                                                                                                      <img alt="Linkedin" height="24" src="data:image/png;base64,{LINKEDIN_IMG}" style="display:block;" width="24">
                                                                                                      </a>
                                                                                                   </td>
                                                                                                </tr>
                                                                                             </tbody>
                                                                                          </table>
                                                                                       </td>
                                                                                    </tr>
                                                                                 </tbody>
                                                                              </table>
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
                                                      <!--[if mso | IE]>
                                                   </td>
                                                   <td style="vertical-align:top;width:304px;">
                                                      <![endif]-->
                                                      <div class="v h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:24.359%;">
                                                         <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                            <tbody>
                                                               <tr>
                                                                  <td style="vertical-align:top;padding:0;">
                                                                     <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                                                        <tbody></tbody>
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
                  <!--[if mso | IE]>
               </td>
            </tr>
         </table>
         <![endif]-->
      </div>
   </body>
</html>
"""
