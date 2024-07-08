// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v5.27.1
// source: data_tranform.proto

package data_transform

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// DataTransformClient is the client API for DataTransform service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DataTransformClient interface {
	// A server-to-client streaming RPC.
	Transform(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (DataTransform_TransformClient, error)
}

type dataTransformClient struct {
	cc grpc.ClientConnInterface
}

func NewDataTransformClient(cc grpc.ClientConnInterface) DataTransformClient {
	return &dataTransformClient{cc}
}

func (c *dataTransformClient) Transform(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (DataTransform_TransformClient, error) {
	stream, err := c.cc.NewStream(ctx, &DataTransform_ServiceDesc.Streams[0], "/data_transform.DataTransform/Transform", opts...)
	if err != nil {
		return nil, err
	}
	x := &dataTransformTransformClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type DataTransform_TransformClient interface {
	Recv() (*QueryOut, error)
	grpc.ClientStream
}

type dataTransformTransformClient struct {
	grpc.ClientStream
}

func (x *dataTransformTransformClient) Recv() (*QueryOut, error) {
	m := new(QueryOut)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// DataTransformServer is the server API for DataTransform service.
// All implementations must embed UnimplementedDataTransformServer
// for forward compatibility
type DataTransformServer interface {
	// A server-to-client streaming RPC.
	Transform(*emptypb.Empty, DataTransform_TransformServer) error
	mustEmbedUnimplementedDataTransformServer()
}

// UnimplementedDataTransformServer must be embedded to have forward compatible implementations.
type UnimplementedDataTransformServer struct {
}

func (UnimplementedDataTransformServer) Transform(*emptypb.Empty, DataTransform_TransformServer) error {
	return status.Errorf(codes.Unimplemented, "method Transform not implemented")
}
func (UnimplementedDataTransformServer) mustEmbedUnimplementedDataTransformServer() {}

// UnsafeDataTransformServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DataTransformServer will
// result in compilation errors.
type UnsafeDataTransformServer interface {
	mustEmbedUnimplementedDataTransformServer()
}

func RegisterDataTransformServer(s grpc.ServiceRegistrar, srv DataTransformServer) {
	s.RegisterService(&DataTransform_ServiceDesc, srv)
}

func _DataTransform_Transform_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DataTransformServer).Transform(m, &dataTransformTransformServer{stream})
}

type DataTransform_TransformServer interface {
	Send(*QueryOut) error
	grpc.ServerStream
}

type dataTransformTransformServer struct {
	grpc.ServerStream
}

func (x *dataTransformTransformServer) Send(m *QueryOut) error {
	return x.ServerStream.SendMsg(m)
}

// DataTransform_ServiceDesc is the grpc.ServiceDesc for DataTransform service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DataTransform_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "data_transform.DataTransform",
	HandlerType: (*DataTransformServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Transform",
			Handler:       _DataTransform_Transform_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "data_tranform.proto",
}