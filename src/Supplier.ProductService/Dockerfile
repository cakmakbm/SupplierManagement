FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
# Directory.Build.props sets TargetFramework; restore fails without it.
COPY ["Directory.Build.props", "./"]
COPY ["src/Shared.Contracts/Shared.Contracts.csproj", "src/Shared.Contracts/"]
COPY ["src/Supplier.ProductService/Supplier.ProductService.csproj", "src/Supplier.ProductService/"]
RUN dotnet restore "src/Supplier.ProductService/Supplier.ProductService.csproj"
COPY . .
RUN dotnet publish "src/Supplier.ProductService/Supplier.ProductService.csproj" -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Supplier.ProductService.dll"]
