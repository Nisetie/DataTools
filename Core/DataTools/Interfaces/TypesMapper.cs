namespace DataTools.Interfaces
{
    /// <summary>
    /// Вспомогательный класс для формализации требований по по преобразованию данных из источника в NET-типы и обратно.
    /// </summary>
    public abstract class TypesMapper
    {
        protected TypesMapper()
        {
            AddLinkBoolean();
            AddLinkBinary();
            AddLinkGuid();
            AddLinkByte();
            AddLinkSByte();
            AddLinkInt16();
            AddLinkInt32();
            AddLinkInt64();
            AddLinkUInt16();
            AddLinkUInt32();
            AddLinkUInt64();
            AddLinkSingle();
            AddLinkDouble();
            AddLinkMoney();
            AddLinkDecimal();
            AddLinkTimestamp();
            AddLinkTimestampTz();
            AddLinkDate();
            AddLinkTime();
            AddLinkString();
            AddLinkAnsiString();
            AddLinkStringFixedLength();
            AddLinkAnsiStringFixedLength();
            AddLinkChar();
            AddLinkJson();
            AddLinkXml();
        }
        protected abstract void AddLinkBoolean();
        protected abstract void AddLinkBinary();
        protected abstract void AddLinkGuid();
        protected abstract void AddLinkByte();
        protected abstract void AddLinkSByte();
        protected abstract void AddLinkInt16();
        protected abstract void AddLinkInt32();
        protected abstract void AddLinkInt64();
        protected abstract void AddLinkUInt16();
        protected abstract void AddLinkUInt32();
        protected abstract void AddLinkUInt64();
        protected abstract void AddLinkSingle();
        protected abstract void AddLinkDouble();
        protected abstract void AddLinkMoney();
        protected abstract void AddLinkDecimal();
        protected abstract void AddLinkTimestamp();
        protected abstract void AddLinkTimestampTz();
        protected abstract void AddLinkDate();
        protected abstract void AddLinkTime();
        protected abstract void AddLinkString();
        protected abstract void AddLinkAnsiString();
        protected abstract void AddLinkStringFixedLength();
        protected abstract void AddLinkAnsiStringFixedLength();
        protected abstract void AddLinkChar();
        protected abstract void AddLinkJson();
        protected abstract void AddLinkXml();
    }
}

