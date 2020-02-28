package main

import (
	"bytes"
	"net/http"
	"net/url"
	"log"
	"strings"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"github.com/antchfx/xmlquery"
)

// our interface
type CacheWriter interface {
	Cache(*awssqs.Message) error
}

// this is our actual implementation
type cacheImpl struct {
	httpClient     *http.Client // our http client connection
	cacheBucket    string       // the bucket we are using for a cache
}

// factory implementation
func NewCacheWriter(config *ServiceConfig) CacheWriter {

	// mock implementation here if necessary

	impl := &cacheImpl{}

	impl.httpClient = newHttpClient(2, config.IIIFServiceTimeout)
    impl.cacheBucket = config.CacheBucketName

	return impl
}

func (c *cacheImpl) Cache(message *awssqs.Message) error {

	// extract the manifest URL otherwise there is nothing to do
	url, err := c.extractManifestUrl( message.Payload )
	if err == nil {

		// did we extract a manifest URL
		if len( url ) != 0 {
			_, err = c.writeManifestToCache( url )

			// dont forget to update the message with the new URL
		}
	} else {
		log.Printf("ERROR: parsing document, no caching possible: %s", err.Error())
		return err
	}

	return err
}

func (c *cacheImpl) writeManifestToCache( url string ) ( string, error ) {

	bucketKey, err := c.makeBucketKey( url )
	if err == nil {
	    body, err := httpGet(url, c.httpClient)
	    if err == nil {
	    	err = s3Add( c.cacheBucket, bucketKey, body )
   	    } else {
		    log.Printf("ERROR: endpoint %s returns %s", url, err)
		    return "", err
	    }
	} else {
		log.Printf("ERROR: parsing URL %s returns %s", url, err)
		return "", err
	}

	return bucketKey, err
}

// extract the manifest URL from the document
func (c *cacheImpl) extractManifestUrl(buffer []byte) (string, error) {

	// generate a query structure from the body
	doc, err := xmlquery.Parse(bytes.NewReader(buffer))
	if err != nil {
		return "", err
	}

	// attempt to extract the url_iiif_manifest_stored field
	manifestUrl := xmlquery.FindOne(doc, "//doc/field[@name='url_iiif_manifest_stored']")
	if manifestUrl == nil {
		// this field is optional so its OK if we dont find it
		return "", nil
	}

	return manifestUrl.InnerText(), nil
}

// make the cache entry bucket key based on the source URL
func (c *cacheImpl) makeBucketKey( sourceUrl string ) ( string, error ) {

	// take the URL, extract the path and translate any special characters

	u, err := url.Parse( sourceUrl )
	if err != nil {
		return "", err
	}

	// ignore the leading slash
	key := strings.ReplaceAll( u.Path[1:], "/", "-" )
	key = strings.ReplaceAll( key, ":", "-" )
	return key, nil

}

//
// end of file
//
