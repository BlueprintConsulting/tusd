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
	//Blob name
	Name string

	AccountName string

	AccountKey string
}

type AzureObjectParams struct {
	// Container specifies the blob container that the object resides in.
	Container string
	// ID specifies the ID of the Azure blob object.
	ID string
}

type AzureBlobReader interface {
	Close() error
	ContentType() string
	Read(p []byte) (int, error)
	Remain() int64
	Size() int64
}

type AzureAPIService interface {
	ReadObject(ctx context.Context) (AzureBlobReader, error)
	GetObjectSize(ctx context.Context) (int64, error)
	SetObjectMetadata(ctx context.Context, params AzureObjectParams, metadata map[string]string) error
	DeleteObject(ctx context.Context, params AzureObjectParams) error
	DeleteObjectsWithFilter(ctx context.Context, ) error
	WriteObject(ctx context.Context) (int64, error)
	ComposeObjects(ctx context.Context) error
	FilterObjects(ctx context.Context) ([]string, error)
}

func (store AzureStore) writeDataToFileInBlob(data []byte, container string, leaseId string, fileName string, isAppend int) error {
	accountName, accountKey := retrieveAccountInfoXX()

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))

	serviceURL := azblob.NewServiceURL(*u, p)

	ctx := context.Background() // This example uses a never-expiring context.
	log.Println("Context is:", ctx)

	containerURL := serviceURL.NewContainerURL(container)
	log.Println("Destination container", containerURL)

	properties, errProp := containerURL.GetProperties(ctx, azblob.LeaseAccessConditions{""})
	log.Println(properties)
	log.Println(errProp)
	if (properties == nil) { //that measn that container does not exist //TODO: cast to azblob.storageError and checkf ro serviceCode == "ContainerNotFound"
		log.Println("Container does not exist. Will be created")
		// Create the container on the service (with no metadata and no public access)
		_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		log.Println("Container created", container)
		if err != nil {
			log.Fatal(err)
		}

	}

	if isAppend == 1 {
		blobURL := containerURL.NewAppendBlobURL(fileName) // Blob names can be mixed case
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
				log.Fatal(err)
			}
		}
	} else {
		blobURL := containerURL.NewBlockBlobURL(fileName) // Blob names can be mixed case
		log.Println("Blob url", blobURL)

		//will do a 0 bytes upload
		_, err = blobURL.Upload(ctx, bytes.NewReader(data), azblob.BlobHTTPHeaders{ContentType: "text/plain"}, azblob.Metadata{}, azblob.BlobAccessConditions{})
		if err != nil {
			log.Fatal(err)
		}
	}

	return err
}

// binPath returns the path to the .bin storing the binary data.
func (store AzureStore) binPath(id string) string {
	return id + ".bin"
}

func (store AzureStore) infoPath(id string) string {
	return id + ".info"
}

// writeInfo updates the entire information. Everything will be overwritten.
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
	accountName, accountKey := retrieveAccountInfoXX()

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))

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
		log.Fatal(err)
	}
	println("Using ioutil all the bytes were read", len(destinationBytes))

	newReaderSeeker := bytes.NewReader(destinationBytes)
	//https://stackoverflow.com/questions/37718191/how-do-i-go-from-io-readcloser-to-io-readseeker#37719717

	blobAccessConditions := azblob.AppendBlobAccessConditions{}
	blobAccessConditions.LeaseID = id

	appendBlockResponse, err := blobURL.AppendBlock(ctx, newReaderSeeker, blobAccessConditions, nil)
	log.Println("Received response", appendBlockResponse)
	if err != nil {
		return 0, err
	}
	return int64(len(destinationBytes)), err
}

func (store AzureStore) LockUpload(id string) error {
	log.Println("[AzureStore.LockUpload]")
	accountName, accountKey := retrieveAccountInfoXX()

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))

	serviceURL := azblob.NewServiceURL(*u, p)
	log.Println("[AzureStore.LockUpload]Service Url", serviceURL)

	ctx := context.Background() // This example uses a never-expiring context.
	log.Println("[AzureStore.LockUpload]Context", ctx)

	containerURL := serviceURL.NewContainerURL(store.Container)
	log.Println("[AzureStore.LockUpload]Destination container", containerURL)

	blobURL := containerURL.NewBlockBlobURL(store.binPath(id)) // Blob names can be mixed case

	var modifiedAccessConditions = azblob.ModifiedAccessConditions{} //TODO create a good lease condition
	/**
	From documentation: https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container
	Specifies the duration of the lease, in seconds, or negative one (-1) for a lease that never expires. A non-infinite lease can be between 15 and 60 seconds.
	 */
	log.Println("[AzureStore.LockUpload] Trying to aquire a lease on blob:", store.binPath(id))
	//TODO: check the duration there
	_, err = blobURL.AcquireLease(ctx, id, -1, modifiedAccessConditions)
	if err != nil {
		currentStack := debug.Stack()
		strStack := string(currentStack)
		log.Fatal("[AzureStore.LockUpload][Error]Stack", strStack, err)
	}
	return err
}

func (store AzureStore) UnlockUpload(id string) error {
	log.Println("[AzureStore.UnlockUpload]")
	accountName, accountKey := retrieveAccountInfoXX()

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))

	serviceURL := azblob.NewServiceURL(*u, p)
	log.Println("[AzureStore.UnlockUpload]Service Url", serviceURL)

	ctx := context.Background() // This example uses a never-expiring context.
	log.Println("[AzureStore.UnlockUpload]Context", ctx)

	containerURL := serviceURL.NewContainerURL(store.Container)
	log.Println("[AzureStore.UnlockUpload]Destination container", containerURL)
	blobURL := containerURL.NewBlockBlobURL(store.binPath(id)) // Blob names can be mixed case

	var modifiedAccessConditions = azblob.ModifiedAccessConditions{} //TODO create a good lease condition
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
	log.Println("[AzureStore.GetInfo]")

	accountName, accountKey := retrieveAccountInfoXX()

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))

	serviceURL := azblob.NewServiceURL(*u, p)
	log.Println("[AzureStore.GetInfo]Service Url", serviceURL)

	ctx := context.Background() // This example uses a never-expiring context.
	log.Println("[AzureStore.GetInfo]Context", ctx)

	containerURL := serviceURL.NewContainerURL(store.Container)
	log.Println("[[AzureStore.GetInfo]] containerURL", containerURL)

	//------------------------ HERE's the part where we read the metadatainfo like in file store--------
	//TODO: use the blob metadata and dont use the fileinfo
	metadataFileInfoURL := containerURL.NewBlockBlobURL(store.infoPath(id)) // Blob names can be mixed case

	get, err := metadataFileInfoURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		log.Fatal(err)
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
			log.Fatal(err)
		}

		marker = listBlob.NextMarker
		log.Println("[AzureStore.GetInfo]", marker)

		//TODO: retrieve the binary blob size not the metadata size
		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			fmt.Println("[AzureStore.GetInfo]Blob name: "+blobInfo.Name+"\n", *blobInfo.Properties.ContentLength)
			if (blobInfo.Name == store.binPath(id)) {
				info.Offset = *blobInfo.Properties.ContentLength
			}
		}
	}
	log.Println("[AzureStore.GetInfo] Info ", info.ID, info.Offset, info.Size)

	return info, nil
}

func retrieveAccountInfoXX() (string, string) {
	//asisu@bpcs.com "conduitteststorage"
	return "conduitteststorage", "1dWptZudG1MmZmHqluxQ1frZJpUwYIXbJ2CQ+THaVP4KimFHglJIPUOdzBrARbT4q8E2NIVOQuGX7gJcdgch+A=="
}
