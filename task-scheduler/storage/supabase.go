package storage

import (
	"log"

	sb "github.com/supabase-community/supabase-go"
)

type SupabaseClient struct {
	client *sb.Client
}

func NewSupabaseClient(url, key string) (*SupabaseClient, error) {
	client, err := sb.NewClient(url, key, nil)
	if err != nil {
		log.Printf("supabase client error %v\n", err)
		return nil, err
	}
	return &SupabaseClient{
		client: client,
	}, nil
}
