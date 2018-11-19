package azure_store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/tus/tusd"
	"github.com/tus/tusd/uid"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"runtime/debug"
)

type AzureStore struct {
	// Specifies the AzureStore Container that uploads will be stored in
	Container string
	//TODO: what would be the use
	//TODO: add logging
	Name string

	AccountName string

	AccountKey string
}

func (store AzureStore) writeDataToFileInBlob(data []byte, container string, leaseId string, fileName string, isAppend int) error {

	credential, err := azblob.NewSharedKeyCredential(store.AccountName, store.AccountKey)
	if err != nil {
		fmt.Errorf("Error encountered %s", err)
		return err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", store.AccountName))

	serviceURL := azblob.NewServiceURL(*u, p)

	ctx := context.Background()

	containerURL := serviceURL.NewContainerURL(container)
	log.Println("Destination container", containerURL)

	properties, errProp := containerURL.GetProperties(ctx, azblob.LeaseAccessConditions{""})
	log.Println(properties)
	log.Println(errProp)
	if properties == nil {
		log.Println("Container does not exist. Will be created")
		_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		log.Println("Container created", container)
		if err != nil {
			fmt.Errorf("Error encountered %s", err)
			return err
		}

	}

	if isAppend == 1 {
		blobURL := containerURL.NewAppendBlobURL(fileName)
		log.Println("Blob url", blobURL)
		if len(data) == 0 {
			resp, err := blobURL.Create(ctx, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
			if err != nil {
				log.Println("Error :", err)
			}
			log.Println(resp)
		} else {
			newReaderSeeker := bytes.NewReader(data)
			blobAccessConditions := azblob.AppendBlobAccessConditions{}
			blobAccessConditions.LeaseID = leaseId

			_, err := blobURL.AppendBlock(ctx, newReaderSeeker, blobAccessConditions, nil)

			if err != nil {
				fmt.Errorf("Error encountered %s", err)
				return err
			}
		}
	} else {
		blobURL := containerURL.NewBlockBlobURL(fileName) // Blob names can be mixed case
		log.Println("Blob url", blobURL)

		//will do a 0 bytes upload
		_, err = blobURL.Upload(ctx, bytes.NewReader(data), azblob.BlobHTTPHeaders{ContentType: "text/plain"}, azblob.Metadata{}, azblob.BlobAccessConditions{})
		if err != nil {
			fmt.Errorf("Error encountered %s", err)
			return err
		}
	}

	return err
}

func (store AzureStore) binPath(id string) string {
	return id + ".bin"
}

func (store AzureStore) infoPath(id string) string {
	return id + ".info"
}

func (store AzureStore) writeInfo(id string, info tusd.FileInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return store.writeDataToFileInBlob(data, store.Container, id, store.infoPath(id), 0)
}

func (store AzureStore) NewUpload(info tusd.FileInfo) (id string, err error) {
	log.Println("[AzureStore.NewUpload]")
	var uploadId string
	if info.ID == "" {
		uploadId = uid.Uid()
	} else {
		// certain tests set info.ID in advance
		uploadId = info.ID
	}

	log.Println("[NewUpload] uploadId:", uploadId)
	info.ID = uploadId

	err = store.writeDataToFileInBlob(make([]byte, 0), store.Container, uploadId, store.binPath(uploadId), 1)

	//log.Println("[NewUpload] creating the metadata file:", store.infoPath(uploadId))
	infoData, err := json.Marshal(info)
	store.writeDataToFileInBlob(infoData, store.Container, uploadId, store.infoPath(uploadId), 0)

	if err != nil {
		return uploadId, err
	}
	return uploadId, err
}

func (store AzureStore) WriteChunk(id string, offset int64, src io.Reader) (int64, error) {
	log.Println("[AzureStore.WriteChunk]")

	credential, err := azblob.NewSharedKeyCredential(store.AccountName, store.AccountKey)
	if err != nil {
		fmt.Errorf("Error encountered %s", err)
		return -1, err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", store.AccountName))

	serviceURL := azblob.NewServiceURL(*u, p)
	log.Println("Service Url", serviceURL)

	ctx := context.Background() // This example uses a never-expiring context.
	log.Println("Context", ctx)

	containerURL := serviceURL.NewContainerURL(store.Container)
	log.Println("Destination container", containerURL)

	blobURL := containerURL.NewAppendBlobURL(store.binPath(id)) // Blob names can be mixed case
	log.Println("Blob url", blobURL)

	destinationBytes, err := ioutil.ReadAll(src)
	if err != nil {
		fmt.Errorf("Error encountered %s", err)
		return -1, err
	}
	println("Using ioutil all the bytes were read", len(destinationBytes))

	newReaderSeeker := bytes.NewReader(destinationBytes)
	//https://stackoverflow.com/questions/37718191/how-do-i-go-from-io-readcloser-to-io-readseeker#37719717

	blobAccessConditions := azblob.AppendBlobAccessConditions{}
	blobAccessConditions.LeaseID = id

	appendBlockResponse, err := blobURL.AppendBlock(ctx, newReaderSeeker, blobAccessConditions, nil)
	log.Println("Received response", appendBlockResponse)
	if err != nil {
		return -1, err
	}
	return int64(len(destinationBytes)), err
}

func (store AzureStore) LockUpload(id string) error {
	log.Println("[AzureStore.LockUpload]")

	credential, err := azblob.NewSharedKeyCredential(store.AccountName, store.AccountKey)
	if err != nil {
		fmt.Errorf("Error encountered %s", err)
		return err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", store.AccountName))

	serviceURL := azblob.NewServiceURL(*u, p)
	log.Println("[AzureStore.LockUpload]Service Url", serviceURL)

	ctx := context.Background() // This example uses a never-expiring context.
	log.Println("[AzureStore.LockUpload]Context", ctx)

	containerURL := serviceURL.NewContainerURL(store.Container)
	log.Println("[AzureStore.LockUpload]Destination container", containerURL)

	blobURL := containerURL.NewBlockBlobURL(store.binPath(id)) // Blob names can be mixed case

	var modifiedAccessConditions = azblob.ModifiedAccessConditions{}
	/**
	From documentation: https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container
	Specifies the duration of the lease, in seconds, or negative one (-1) for a lease that never expires. A non-infinite lease can be between 15 and 60 seconds.
	 */
	log.Println("[AzureStore.LockUpload] Trying to aquire a lease on blob:", store.binPath(id))

	_, err = blobURL.AcquireLease(ctx, id, -1, modifiedAccessConditions)
	if err != nil {
		currentStack := debug.Stack()
		strStack := string(currentStack)
		fmt.Errorf("Error encountered %s", "[AzureStore.LockUpload][Error]Stack", strStack, err)
		return err
	}
	return err
}

func (store AzureStore) UnlockUpload(id string) error {
	log.Println("[AzureStore.UnlockUpload]")

	credential, err := azblob.NewSharedKeyCredential(store.AccountName, store.AccountKey)
	if err != nil {
		fmt.Errorf("Error encountered %s", err)
		return err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", store.AccountName))

	serviceURL := azblob.NewServiceURL(*u, p)
	log.Println("[AzureStore.UnlockUpload]Service Url", serviceURL)

	ctx := context.Background() // This example uses a never-expiring context.
	log.Println("[AzureStore.UnlockUpload]Context", ctx)

	containerURL := serviceURL.NewContainerURL(store.Container)
	log.Println("[AzureStore.UnlockUpload]Destination container", containerURL)
	blobURL := containerURL.NewBlockBlobURL(store.binPath(id)) // Blob names can be mixed case

	var modifiedAccessConditions = azblob.ModifiedAccessConditions{}
	_, err = blobURL.ReleaseLease(ctx, id, modifiedAccessConditions)

	return err
}

func (store AzureStore) ConcatUploads(dest string, uploads []string) (err error) {
	log.Println("[AzureStore.ConcatUploads]")
	return nil
}

// UseIn sets this store as the core data store in the passed composer and adds
// all possible extension to it.
func (store AzureStore) UseIn(composer *tusd.StoreComposer) {
	composer.UseCore(store)
	composer.UseGetReader(store)
	composer.UseTerminater(store)
	composer.UseLocker(store)
	composer.UseConcater(store)
}

func (store AzureStore) GetReader(id string) (io.Reader, error) {
	return os.Open(store.binPath(id))
}

func (store AzureStore) Terminate(id string) error {
	if err := os.Remove(store.infoPath(id)); err != nil {
		return err
	}
	if err := os.Remove(store.binPath(id)); err != nil {
		return err
	}
	return nil
}

func (store AzureStore) GetInfo(id string) (tusd.FileInfo, error) {
	log.Println("[AzureStore.GetInfo] id", id)

	credential, err := azblob.NewSharedKeyCredential(store.AccountName, store.AccountKey)
	if err != nil {
		fmt.Errorf("Error encountered %s", err)
		return tusd.FileInfo{},err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", store.AccountName))

	serviceURL := azblob.NewServiceURL(*u, p)
	log.Println("[AzureStore.GetInfo]Service Url", serviceURL)

	ctx := context.Background() // This example uses a never-expiring context.
	log.Println("[AzureStore.GetInfo]Context", ctx)

	containerURL := serviceURL.NewContainerURL(store.Container)
	log.Println("[[AzureStore.GetInfo]] containerURL", containerURL)

	metadataFileInfoURL := containerURL.NewBlockBlobURL(store.infoPath(id))

	get, err := metadataFileInfoURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		fmt.Errorf("Error encountered %s", err)
		return tusd.FileInfo{},err
	}

	info := tusd.FileInfo{ID: id}

	downloadedData := &bytes.Buffer{}
	reader := get.Body(azblob.RetryReaderOptions{})
	downloadedData.ReadFrom(reader)
	reader.Close()
	if err := json.Unmarshal(downloadedData.Bytes(), &info); err != nil {
		return info, err
	}

	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: id})
		if err != nil {
			fmt.Errorf("Error encountered %s", err)
			return tusd.FileInfo{},err
		}

		marker = listBlob.NextMarker
		log.Println("[AzureStore.GetInfo]", marker)

		for _, blobInfo := range listBlob.Segment.BlobItems {
			fmt.Println("[AzureStore.GetInfo]Blob name: "+blobInfo.Name+"\n", *blobInfo.Properties.ContentLength)
			if blobInfo.Name == store.binPath(id) {
				info.Offset = *blobInfo.Properties.ContentLength
			}
		}
	}
	log.Println("[AzureStore.GetInfo] Info ", info.ID, info.Offset, info.Size)

	return info, nil
}
