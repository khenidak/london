package backend

import (
	"github.com/Azure/azure-sdk-for-go/storage"
)

func ToStorgeError(e error) *storage.AzureStorageServiceError {
	aze, ok := e.(storage.AzureStorageServiceError)
	if !ok {
		return nil
	}

	return &aze
}

func IsNotFoundError(e error) bool {
	if e == nil {
		return false
	}
	aze := ToStorgeError(e)
	if aze == nil {
		return false
	}

	return aze.StatusCode == 404

}

func IsConflictError(e error) bool {
	if e == nil {
		return false
	}
	aze := ToStorgeError(e)
	if aze == nil {
		return false
	}

	return aze.StatusCode == 409
}

func IsEntityAlreadyExists(e error) bool {
	if e == nil {
		return false
	}
	aze := ToStorgeError(e)
	if aze == nil {
		return false
	}

	return aze.StatusCode == 409 && aze.Code == "EntityAlreadyExists"
}
