using DataTools.Interfaces;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace DataTools.Common
{
    public class DynamicModel : DynamicObject, IDictionary<string, object>, INotifyPropertyChanged
    {
        private IModelMetadata _modelMetadata;
        private Dictionary<string, object> _members;
        public event PropertyChangedEventHandler PropertyChanged;
        public ICollection<string> Keys => _members.Keys;
        public ICollection<object> Values => _members.Values;
        public int Count => _members.Count;
        public bool IsReadOnly => false;
        public object this[string key]
        {
            get => _members.TryGetValue(key, out object value) ? value : null;
            set
            {
                _members[key] = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(key));
            }
        }
        public IModelMetadata ModelMetadata => _modelMetadata;
        private DynamicModel()
        {
            _members = new Dictionary<string, object>();
        }
        public DynamicModel(IModelMetadata modelMetadata) : this()
        {
            _modelMetadata = modelMetadata;
            foreach (var f in _modelMetadata.Fields)
                _members[f.FieldName] = null;
        }

        public override IEnumerable<string> GetDynamicMemberNames()
        {
            return _members.Keys;
        }

        public override string ToString()
        {
            var presentation = new StringBuilder();
            foreach (var f in ModelMetadata.Fields)
                if (f.IsPresentation)
                    presentation.Append($"{(_members[f.FieldName] == null ? "NULL" : _members[f.FieldName])};");
            if (presentation.Length == 0)
                foreach (var f in ModelMetadata.Fields)
                    if (f.IsPrimaryKey || f.IsUnique || f.IsAutoincrement)
                        presentation.Append($"{(_members[f.FieldName] == null ? "NULL" : _members[f.FieldName])};");
            if (presentation.Length == 0)
                foreach (var f in ModelMetadata.Fields)
                    presentation.Append($"{(_members[f.FieldName] == null ? "NULL" : _members[f.FieldName])};");
            if (presentation.Length == 0) return string.Empty;
            else
            {
                presentation.Length -= 1;
                return presentation.ToString();
            }
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            if (_members.TryGetValue(binder.Name, out result))
                return true;
            else
            {
                result = null;
                return true;
            }
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            _members[binder.Name] = value;
            return true;
        }

        public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
        {
            if (_members.ContainsKey(binder.Name))
            {
                dynamic invoker = _members[binder.Name];
                result = (invoker as Delegate).DynamicInvoke(args);
                return true;
            }
            else
            {
                result = null;
                return false;
            }
        }

        public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
        {
            result = null;
            if (indexes.Length != 1)
                return false;

            object index = indexes[0];
            if (index is string key)
                result = _members[key];
            else return false;

            return true;
        }

        public override bool TrySetIndex(SetIndexBinder binder, object[] indexes, object value)
        {
            if (indexes.Length != 1)
                return false;

            object index = indexes[0];
            if (index is string key)
                _members[key] = value;
            else return false;

            return true;
        }

        public void Add(string key, object value)
        {
            _members[key] = value;
        }

        public bool ContainsKey(string key)
        {
            return _members.ContainsKey(key);
        }

        public bool Remove(string key)
        {
            return _members.Remove(key);
        }

        public bool TryGetValue(string key, out object value)
        {
            return _members.TryGetValue(key, out value);
        }

        public void Add(KeyValuePair<string, object> item)
        {
            _members[item.Key] = item.Value;
        }

        public void Clear()
        {
            _members.Clear();
        }

        public bool Contains(KeyValuePair<string, object> item)
        {
            return _members.Contains(item);
        }

        public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
        {
            int i = 0;
            foreach (var kv in _members)
                array[arrayIndex + i++] = kv;
        }

        public bool Remove(KeyValuePair<string, object> item)
        {
            return _members.Remove(item.Key);
        }

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            return _members.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _members.GetEnumerator();
        }
    }
}

