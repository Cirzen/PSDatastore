using namespace System.Collections.Generic;

class DSUri
{
    [string]$Project
    [Dictionary[string, hashtable]]$UriList
        
    DSUri([string]$Project)
    {
        if ([string]::IsNullorWhiteSpace($Project))
        {
            throw [ArgumentException]::new("Project cannot be null or empty string")
        }
        $this.Project = $Project
        $this.PopulateUris()
    }

    PopulateUris()
    {
        [string]$UriBase = "https://datastore.googleapis.com/v1/projects/$($this.Project):"
        
        $this.UriList = New-Object "Dictionary[string, hashtable]" -ArgumentList ([StringComparer]::OrdinalIgnoreCase)


        $this.UriList.Add("RunQuery", @{Uri = $URIBase + "runQuery"; method = "Post"})
        $this.UriList.Add("BeginTransaction", @{Uri = $URIBase + "beginTransaction"; method = "Post"})
        $this.UriList.Add("Lookup", @{Uri = $URIBase + "lookup"; method = "Post"})
        $this.UriList.Add("AllocateIds", @{Uri = $URIBase + "allocateIds"; method = "Post"})
        $this.UriList.Add("Commit", @{Uri = $URIBase + "commit"; method = "Post"})
        $this.UriList.Add("ReserveIds", @{Uri = $URIBase + "reserveIds"; method = "Post"})
        $this.UriList.Add("Rollback", @{Uri = $URIBase + "rollback"; method = "Post"})
    }

    [string] GetUri([string]$Action)
    {
        If (!$this.UriList.ContainsKey($Action))
        {
            Throw [System.ArgumentException]::new("Unknown Action: $Action", "Action")
        }
        return $this.UriList[$Action]["Uri"]
    }

    [string] GetMethod([string]$Action)
    {     
        If (!$this.UriList.ContainsKey($Action))
        {
            Throw [System.ArgumentException]::new("Unknown Action: $Action", "Action")
        }
        return $this.UriList[$Action]["method"]
    }

    [object] InvokeRestMethod([string]$Action, [object]$Body)
    {
        return (Invoke-RestMethod -Authentication OAuth -Token  -Method $this.GetMethod($Action) -Uri $this.GetUri($Action) -ContentType "application/json" -Body $Body)
    }
}

class PartitionId
{
    [string]$projectId;
    [string]$namespaceId;

    PartitionId([string]$project)
    {
        $this.projectId = $project
    }

    PartitionId([string]$project, [string]$namespace)
    {
        $this.projectId = $project
        $this.namespaceId = $namespace
    }
}

class TransactionOptions
{
    [ReadWrite]$ReadWrite
    [ReadOnly]$ReadOnly

    TransactionOptions()
    {}

    TransactionOptions([bool]$readWrite)
    {
        if ($readwrite)
        {
            $this.ReadWrite = [ReadWrite]::new();
        }
        else {
            $this.ReadOnly = [ReadOnly]::new();
        }
    }

    TransactionOptions([string]$previousTransaction)
    {
        $this.ReadWrite = [ReadWrite]::new($previousTransaction)
    }
}

class ReadWrite
{
    [string]$PreviousTransaction

    ReadWrite(){}

    ReadWrite([string]$prevTransaction)
    {
        $this.PreviousTransaction = $prevTransaction
    }
}

class ReadOnly
{

}

class PathElement
{
    [string]$Kind
    [string]$Id
    [string]$Name

    PathElement([string]$kind, [int64]$id)
    {
        if ($id -le 0)
        {
            throw "id must be a positive integer"
        }

        $this.Kind = $kind
        $this.Id = $id.ToString()
    }

    PathElement([string]$kind, [string]$name)
    {
        $this.Kind = $kind
        $this.Name = $name
    }

    hidden PathElement([string]$kind)
    {
        $this.Kind = $kind
    }

    # Design choice. Rather than have the constructor permit this, the method name
    # stresses to the user that this is only a partially constructed element.
    static [PathElement]CreateIncompleteElement([string]$kind)
    {
        return [PathElement]::new($kind)
    }
}


class Key
{
    [PartitionId]$partitionId;
    [List[PathElement]]$path;

    Key([PartitionId]$partitionId, [PathElement[]]$pathElement)
    {
        $this.partitionId = $partitionId
        $this.path = New-Object 'List[PathElement]'
        ForEach ($pe in $pathElement)
        {
            $this.path.Add($pe)
        }
    }

    # Create Incomplete Key
    # For use by KeyFactory class
    hidden Key([PartitionId]$partitionId, [string]$kind)
    {
        $this.partitionId = $partitionId
        $this.path = [PathElement]::CreateIncompleteElement($kind)
    }

    [bool]IsComplete()
    {
        return ($null -ne $this.path -AND $null -ne $this.path[-1].Name) -OR ($null -ne $this.path -AND $null -ne $this.path[-1].Id -AND $this.path[-1].Id -ne 0 )
    }
}

class KeyFactory
{
    [string]$Project
    [string]$Kind
    [string]$Namespace

    KeyFactory([string]$project, [string]$kind)
    {
        $this.Kind = $kind
        $this.Project = $project
    }

    KeyFactory([string]$project, [string]$kind, [string]$namespace)
    {
        $this.Kind = $kind
        $this.Project = $project
        $this.Namespace = $namespace
    }

    [Key]CreateIncompleteKey()
    {
        return [Key]::new($this.CreatePartition(), $this.Kind)
    }

    [Key]CreateKey([string]$name)
    {
        return [Key]::new($this.CreatePartition(), [PathElement]::new($this.Kind, $name))
    }

    [Key]CreateKey([int64]$id)
    {
        return [Key]::new($this.CreatePartition(), [PathElement]::new($this.Kind, $id))
    }

    hidden [PartitionId]CreatePartition()
    {

        $Partition = [PartitionId]::new()
        $Partition.projectId = $this.Project
        if ($this.Namespace)
        {
            $Partition.namespaceId = $this.Namespace
        }
        return $Partition
    }
}

enum ReadConsistency
{
    STRONG = 0;
    EVENTUAL = 1;
}

class ReadOptions
{
    #Mandatory
    [string]$transaction
    #Optional
    [ReadConsistency]$readConsistency

    ReadOptions([string]$transactionId)
    {
        $this.transaction = $transactionId
    }
}

class GqlQuery
{
    [string]$queryString
    [bool]$allowLiterals = $false
    [Dictionary[string, object]]$namedBindings
    [List[object]]$positionalBindings

    GqlQuery(){}

    GqlQuery([string]$queryString)
    {
        $this.queryString = $queryString
    }
}

class Value
{
    
    [int]$meaning
    [bool]$excludeFromIndexes
    
    # Only one of the following may be set:
    [object]$nullValue
    [bool]$booleanValue
    [string]$integerValue
    [double]$doubleValue
    [string]$timestampValue
    [key]$keyValue
    [string]$stringValue
    [string]$blobValue
    [LatLng]$geoPointValue
    [Entity]$entityValue
    [ArrayValue]$arrayValue

    Value([object]$value)
    {
        SetProperty($value)
    }

    [void]SetProperty([object]$property)
    {
        try
        {
            switch ($property.GetType())
            {
                {$_ -eq [bool]}
                {
                    $this.booleanValue = $property
                    break
                }
                {[int64]::TryParse($property.ToString(), [ref]$null)}
                {
                    $this.integerValue = $property.ToString()
                    break
                }
                {[uint64]::TryParse($property.ToString(), [ref]$null)}
                {
                    $this.integerValue = $property.ToString()
                    break
                }
                {[double]::TryParse($property.ToString(), [ref]$null)}
                {
                    $this.doubleValue = $property.ToString()
                    break
                }
                {$_ -is [datetime]}
                {
                    $this.timestampValue = [System.Xml.XmlConvert]::ToString($property, [System.Xml.XmlDateTimeSerializationMode]::Utc)
                    break
                }
                {$_ -eq [Key]}
                {
                    $this.keyValue = $property
                    break
                }
                {$_ -is [string]}
                {
                    $this.stringValue = $property
                    break
                }
                {$_ -is [byte[]]}
                {
                    $this.blobValue = [System.Convert]::ToBase64String($property)
                    break
                }
                {$_ -eq [LatLng]}
                {
                    $this.geoPointValue = $property
                    break
                }
                {$_ -eq [Entity]}
                {
                    $this.entityValue = $property
                    break
                }
                {$_.IsArray} # Needs testing!
                {
                    $List = New-Object 'List[Value]'
                    ForEach ($val in $property)
                    {
                        if ($null -ne $val -AND $val.GetType().IsArray)
                        {
                            throw "ArrayValue cannot contain another ArrayValue"
                        }
                        $List.Add(([Value]::new($val))) # Recursive Constructor
                        
                    }
                    $this.arrayValue = [ArrayValue]::new($List)
                    break
                }
                Default
                {
                    throw "Unhandled type in Value"
                }
            }
        }
        catch [System.Management.Automation.RuntimeException]
        {
            if ($_.exception.message -eq "You cannot call a method on a null-valued expression.")
            {
                $this.nullValue = $null
            }
        }
    }
}

class Entity
{
    [Key]$key
    [Dictionary[string, Value]]$properties

    Entity([Key]$key, [Dictionary[string, Value]]$properties)
    {
        $this.key = $key
        $this.properties = $properties
    }

    Entity([Key]$key, [Dictionary[string, object]]$properties)
    {
        $this.key = $key
        $propDictionary = New-Object 'Dictionary[string, Value]'
        ForEach ($p in $properties.GetEnumerator())
        {
            $propDictionary.Add($p.Key, [Value]::new($p.Value))
        }
        $this.properties = $propDictionary
    }

    Entity([Key]$key, [hashtable]$properties)
    {
        # Veryify that all keys in the hashtable are strings
        if (($properties.keys.ForEach( {$_.GetType.Name}) -notmatch "String").Count -gt 0)
        {
            throw [System.Data.DataException]::new("All hashtable keys must be of type String)")
        }
        $this.key = $key
        $propDictionary = New-Object 'Dictionary[string, Value]'
        ForEach ($p in $properties.GetEnumerator())
        {
            $propDictionary.Add($p.Key, [Value]::new($p.Value))
        }
        $this.properties = $propDictionary
    }
}

enum MutationAction
{
    Insert
    Update
    Upsert
    Delete
}

class Mutation
{
    # Class may contain only one of the following entity actions:
    [Entity]$insert
    [Entity]$update
    [Entity]$upsert
    [Key]$delete
    
    # And this is optional
    [string]$baseVersion

    Mutation([Entity]$entity, [MutationAction]$action, [string]$baseVersion = [string]::empty)
    {
        if (![string]::IsNullOrWhiteSpace($baseVersion))
        {
            $this.baseVersion = $baseVersion
        }

        Switch ($action)
        {
            "Insert"
            {
                $this.insert = $entity
                break
            }
            "Update"
            {
                $this.update = $entity
                break
            }
            "Upsert"
            {
                $this.upsert = $entity
                break
            }
            Default
            {
                Throw [System.ArgumentException]::new("Unexpected action" , "action")
            }
        }
    }

    Mutation([Key]$key_to_delete, [string]$baseVersion = [string]::empty)
    {
        if (![string]::IsNullOrWhiteSpace($baseVersion))
        {
            $this.baseVersion = $baseVersion
        }
        $this.delete = $key_to_delete
    }

}

enum CommitMode
{
    TRANSACTIONAL;
    NON_TRANSACTIONAL;
}

class CommitBody
{
    [CommitMode]$mode
    [List[Mutation]]$mutations
    [string]$transaction

    CommitBody([string]$transactionId)
    {
        $this.mode = [CommitMode]::TRANSACTIONAL
        $this.transaction = $transactionId
    }
    
    CommitBody([string]$transactionId, [mutation[]]$mutations, [CommitMode]$mode = [CommitMode]::TRANSACTIONAL)
    {
        $this.transaction = $transactionId
        $this.mode = $mode
        ForEach ($m in $mutations)
        {
            $this.AddMutation($m)
        }
    }

    AddMutation([Mutation]$mutation)
    {
        $this.mutations.Add($mutation)
    }

    [bool]IsReadyToCommit()
    {
        try {
            return ($this.transaction.length -gt 0 -AND $this.transaction.Length % 4 -eq 0 -AND $this.mutations.Count -gt 0)
        }
        catch {
            return $false
        }
    }

    [Object]Commit([DSUri]$dsUri)
    {
        return $dsUri.InvokeRestMethod("Commit", $this)
    }

}

class LookupBody
{
    [ReadOptions]$readOptions
    [List[Key]]$keys

    LookupBody([string]$transactionId)
    {
        $this.readOptions = [ReadOptions]::new($transactionId)
    }

    AddKey([Key]$key)
    {
        $this.keys.Add($key)
    }
}


enum GeoLocationAxis
{
    Latitude
    Longitude
}

class LatLng
{
    [double]$longitude #{get; private set}
    [double]$latitude #{get; private set}

    LatLng([double]$l, [GeoLocationAxis]$axis)
    {
        switch ($axis)
        {
            "Latitude"
            {

                $this.SetLatitude($l)
            }
            "Longitude"
            {

                $this.SetLongitude($l)

            }
            Default
            {
                Throw [System.ArgumentException]::new("Invalid Axis")
            }
        }
    }

    LatLng([double]$latitude, [double]$longitude)
    {
        $this.SetLatitude($latitude)
        $this.SetLongitude($longitude)
    }

    SetLatitude([double]$latitude)
    {
        if ($latitude -le 90 -and $latitude -ge -90)
        {
            $this.latitude = $latitude
            return
        }
        throw [System.ArgumentException]("Latitude must be in the range [-90.0, 90.0]", "latitude")
    }
    SetLongitude([double]$longitude)
    {
        if ($longitude -le 180 -and $longitude -ge -180)
        {
            $this.longitude = $longitude
            return
        }
        throw [System.ArgumentException]("Longitude must be in the range [-180.0, 180.0]", "longitude")
    }
}

class ArrayValue
{
    [List[Value]]$values

    ArrayValue([Value[]]$values)
    {
        ForEach ($v in $values)
        {
            $this.values.Add($v)
        }
    }

    ArrayValue([List[Value]]$values)
    {
        $this.values = $values
    }
}






####  #   # ####      ### #     ###   ###   ###  ####   ###
#     ##  # #   #    #    #    #   # #     #     #     #
###   # # # #   #    #    #    #####  ###   ###  ###    ###
#     #  ## #   #    #    #    #   #     #     # #         #
##### #   # ####      ### #### #   #  ###   ###  #####  ###


function New-Mutation
{
    throw [System.NotImplementedException]::new()
}

function Invoke-GdsRollback
{
    Param(
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)]
        [string]$TransactionId
    )
    throw [System.NotImplementedException]::new()
}

function Invoke-GdsBeginTransaction
{
param (
    # The project ID against which to begin the transaction
    [Parameter(Mandatory = $true)]
    [string]$ProjectId,

    # The Transaction option. Must be "ReadOnly" or "ReadWrite"
    [Parameter(Mandatory = $false)]
    [ValidateSet("ReadOnly", "ReadWrite")]
    [Alias("Options")]
    [string]$TransactionOptions = "ReadOnly",

    # For ReadWrite transactions, the transaction identifier of the transaction being retried (optional)
    [Parameter(Mandatory = $false)]
    [string]$PreviousTransaction


)

Begin {
    $UriList = [DSUri]::new($ProjectId);
}

Process
{
    $TO = [TransactionOptions]::new()
    Switch ($TransactionOptions)
    {
        "ReadOnly"
        {
            $TO.ReadOnly = [ReadOnly]::new()
        }
        "ReadWrite"
        {
            $TO.ReadWrite = [ReadWrite]::new()
            if ($PreviousTransaction)
            {
                $TO.ReadWrite.PreviousTransaction = $PreviousTransaction
            }
        }
        Default
        {
            Throw [System.ArgumentException]::new("Unexpected TransactionOptions value", "TransactionOptions")
        }
    }

    $Return = $UriList.InvokeRestMethod("BeginTransaction", $TO)
    return $Return
}

End {

}

}

function ConvertFrom-Base64
{
    param(
        [Parameter(Mandatory = $true, ValueFromPipeline = $true, Position = 0)]
        [string]$Base64,

        [switch]$AsText
    )
    $mod4 = $Base64.Length % 4

    if ($mod4 -eq 1)
    {
        throw "Invalid length of Base64 string"
    }
    if ($mod4 -gt 0)
    {
        $Base64 += [String]::new('=', 4 - $mod4)
    }

    $bytes = [Convert]::FromBase64String($Base64)
    if ($AsText)
    {
        return [Text.UTF8Encoding]::new().GetString($bytes)
    }
    return $bytes
}


function Invoke-GdsGqlQuery
{
Param(
    [string]$Project,
    [string]$QueryString,
    [bool]$AllowLiterals,
    [hashtable]$NamedBindings,
    [List[Object]]$PositionalBindings
)
Begin{
    $UriList = [DSUri]::new($Project)
}

Process{
    $Query = [GqlQuery]::new()
    $Query.queryString = $QueryString
    $Query.allowLiterals = $AllowLiterals
    $Query.namedBindings = $NamedBindings
    $Query.positionalBindings = $PositionalBindings

    $UriList.InvokeRestMethod("runQuery", $Query)
}

End{}
}

function Get-GdsEntity #Lookup
{
    Param(
        [string]
        $ProjectId,
        [string]
        $TransactionId,

        [Key[]]
        $Key
    )

    Begin
    {
        $DsUri = [DSUri]::new($ProjectId)
    }
    End
    {
        $Body = [LookupBody]::new($TransactionId)
        ForEach ($k in $Key)
        {
            $Body.AddKey($k)
        }

        $DsUri.InvokeRestMethod("Lookup", $Body)
    }
}

function New-GdsCommit
{
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [string]
        $TransactionId,
        
        [Parameter(Mandatory = $false)]
        [switch]
        $NonTransactional,
        
        [Parameter(Mandatory = $false, ValueFromPipeline = $true)]
        [Mutations[]]
        $Mutations
    )

    Process
    {
        $Body = [CommitBody]::new($TransactionId)
        if ($NonTransactional) {$Body.mode = [CommitMode]::NON_TRANSACTIONAL}
        ForEach ($m in $Mutations)
        {
            $Body.AddMutation($m)
        }

        
    }
    End
    {
        return $Body
    }
}

function Invoke-GdsCommit
{
    Param(
        [string]$Project,
        [CommitBody]$CommitBody
    )

    Begin
    {
        $UriList = [DSUri]::new($Project)
    }

    Process
    {
        if ($CommitBody.IsReadyToCommit())
        {
            return $CommitBody.Commit($UriList)
        }
        throw "CommitBody object not in valid state to commit"
    }

}



