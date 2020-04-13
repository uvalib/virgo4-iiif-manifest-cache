package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"log"
	"strings"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"github.com/antchfx/xmlquery"
)

var badManifestValue = "???????"

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
	manifestUrl, err := c.extractManifestUrl( message.Payload )
	if err == nil {

		// did we extract a manifest URL that makes sense
		if len( manifestUrl ) != 0 && strings.Contains( manifestUrl, badManifestValue ) == false {

			// TEMP ONLY
			//manifestUrl = strings.Replace( manifestUrl, "https://iiifman.lib.virginia.edu", "https://iiif-manifest-dev.internal.lib.virginia.edu", 1 );

			newUrl, err := c.writeManifestToCache( manifestUrl )

			// if successful, update the payload with the new URL
			if err == nil {

				log.Printf( "INFO: Rewriting manifest URL from %s -> %s", manifestUrl, newUrl )

				payload := string( message.Payload )
				payload = strings.Replace( payload,
					fmt.Sprintf( ">%s<", manifestUrl ),
					fmt.Sprintf( ">%s<", newUrl ), 1 )
				message.Payload = []byte(payload)
			}
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
	    	if err == nil {
	    		newUrl := fmt.Sprintf( "https://%s.s3.amazonaws.com/%s", c.cacheBucket, bucketKey )
	    		return newUrl, nil
			}
   	    } else {
		    log.Printf("ERROR: endpoint %s returns %s", url, err)
	    }
	} else {
		log.Printf("ERROR: parsing URL %s returns %s", url, err)
	}

	return "", err
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
