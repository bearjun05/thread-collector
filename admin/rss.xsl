<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:content="http://purl.org/rss/1.0/modules/content/"
  xmlns:media="http://search.yahoo.com/mrss/">

  <xsl:output method="html" encoding="UTF-8" indent="yes" />

  <xsl:template match="/">
    <html>
      <head>
        <title>Thread Collector RSS</title>
        <style>
          body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #f7f4ee; color: #1a1a1a; padding: 24px; }
          .item { background: #fff; border: 1px solid #e5dcc8; border-radius: 12px; padding: 16px; margin-bottom: 16px; }
          .title { font-size: 18px; font-weight: 700; margin: 0 0 8px; }
          .meta { color: #666; font-size: 12px; margin-bottom: 12px; }
          .content img { max-width: 100%; border-radius: 10px; margin: 8px 0; }
          .content video { max-width: 100%; margin: 8px 0; }
        </style>
      </head>
      <body>
        <h1>
          <xsl:value-of select="//*[local-name()='channel']/*[local-name()='title']" />
        </h1>
        <xsl:choose>
          <xsl:when test="count(//*[local-name()='item']) &gt; 0">
            <xsl:for-each select="//*[local-name()='item']">
          <div class="item">
            <div class="title"><a href="{*[local-name()='link']}"><xsl:value-of select="*[local-name()='title']" /></a></div>
            <div class="meta"><xsl:value-of select="*[local-name()='pubDate']" /></div>
            <div class="content">
              <xsl:choose>
                <xsl:when test="content:encoded">
                  <xsl:value-of select="content:encoded" disable-output-escaping="yes" />
                </xsl:when>
                <xsl:otherwise>
                  <xsl:value-of select="*[local-name()='description']" disable-output-escaping="yes" />
                </xsl:otherwise>
              </xsl:choose>
            </div>
          </div>
            </xsl:for-each>
          </xsl:when>
          <xsl:otherwise>
            <div class="item">No items.</div>
          </xsl:otherwise>
        </xsl:choose>
      </body>
    </html>
  </xsl:template>
</xsl:stylesheet>
